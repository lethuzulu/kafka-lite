use kafka_lite_protocol::request::{Action, Request, decode_request};
use kafka_lite_protocol::response::{
    ResponseError, ResponseKind, SuccessBody, SuccessType, encode_response,
};
use crate::store::broker::Broker;
use anyhow::Result;
use std::io::BufRead;
use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use log::{error, info, warn};

const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct TcpServer {
    inner: TcpListener,
    broker: Arc<Mutex<Broker>>,
    condvar: Arc<Condvar>,
}

impl TcpServer {
    pub fn try_new(address: impl ToSocketAddrs, broker: Broker) -> Result<Self> {
        let inner = TcpListener::bind(address)?;
        let broker = Arc::new(Mutex::new(broker));
        let condvar = Arc::new(Condvar::new());

        info!("Tcp started...");

        Ok(Self { inner, broker, condvar })
    }

    pub fn listen(&self) {
        for conn in self.inner.incoming() {
            match conn {
                Ok(stream) => {
                    let broker = Arc::clone(&self.broker);
                    let condvar = Arc::clone(&self.condvar);
                    thread::spawn(|| handle_connection(stream, broker, condvar));
                }
                Err(e) => warn!("error occurred. listening for next connection: {}", e),
            }
        }
    }
}

fn handle_connection(stream: TcpStream, broker: Arc<Mutex<Broker>>, condvar: Arc<Condvar>) {
    let mut line = String::new();

    let mut writer = match stream.try_clone() {
        Ok(w) => w,
        Err(e) => {
            error!("failed to clone stream: {}", e);
            return;
        }
    };
    let mut buf_reader = BufReader::new(stream);

    loop {
        match buf_reader.read_line(&mut line) {
            Ok(0) => break, //client disconnected
            Ok(_) => {}
            Err(e) => {
                error!("read error: {}", e);
                break;
            }
        }
        let request = match decode_request(&line) {
            Ok(r) => r,
            Err(e) => {
                warn!("error, failed to deserialize request: {}", e);

                let err = ResponseKind::Err(ResponseError {
                    message: "bad request".to_string(),
                });

                let res = match encode_response(err) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("failure to serialize the response {}", e);
                        break;
                    }
                };
                if let Err(e) = writer.write_all(res.as_bytes()) {
                    error!("failed to write to socket: {}", e);
                }
                line.clear();
                continue;
            }
        };

        let result = handle_request(request, &broker, &condvar);
        let response = match encode_response(result) {
            Ok(response) => response,
            Err(e) => {
                error!("failed to encode response: {}", e);
                break;
            }
        };
        if let Err(e) = writer.write_all(response.as_bytes()) {
            error!("failed to write to socket: {}", e);
        }
        line.clear()
    }
}

fn handle_request(req: Request, broker: &Arc<Mutex<Broker>>, condvar: &Arc<Condvar>) -> ResponseKind {
    match req.action {
        Action::Write { topic, payload } => {
            let result = {
                let mut broker = broker.lock().unwrap_or_else(|e| e.into_inner());
                broker.append(&topic, &payload)
            };
            condvar.notify_all();
            match result {
                Ok(offset) => ResponseKind::Ok(SuccessBody {
                    data: SuccessType::Write { offset },
                }),
                Err(_) => ResponseKind::Err(ResponseError {
                    message: "internal error".into(),
                }),
            }
        }
        Action::Read { topic, consumer_id } => {
            let deadline = Instant::now() + LONG_POLL_TIMEOUT;
            let mut guard = broker.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                match guard.read_from(&topic, &consumer_id) {
                    Err(_) => {
                        return ResponseKind::Err(ResponseError {message: "topic or consumer_id does not exist".to_string()});
                    }
                    Ok(r) if !r.messages.is_empty() => {
                        return ResponseKind::Ok(SuccessBody {
                            data: SuccessType::Read {
                                messages: r.messages,
                                next_offset: r.next_offset
                            }
                        });
                    }
                    Ok(r) => {
                        let remaining = match deadline.checked_duration_since(Instant::now()) {
                            Some(d) => d,
                            None => {
                                return ResponseKind::Ok(SuccessBody {
                                    data: SuccessType::Read {
                                        messages: r.messages,
                                        next_offset: r.next_offset
                                    }
                                });
                            }
                        };
                        let (new_guard, _) = condvar.wait_timeout(guard, remaining).unwrap_or_else(|e| e.into_inner());
                        guard = new_guard;
                    }
                }
            }
        }
        Action::Commit {
            topic,
            consumer_id,
            offset,
        } => {
            let mut broker = broker.lock().unwrap_or_else(|e| e.into_inner());
            match broker.commit_offset(&topic, &consumer_id, offset) {
                Ok(offset) => ResponseKind::Ok(SuccessBody {
                    data: SuccessType::Commit { offset },
                }),
                Err(e) => ResponseKind::Err(ResponseError { message: e.to_string() }),
            }
        }
        Action::CreateTopic { name } => {
            let mut broker = broker.lock().unwrap_or_else(|e| e.into_inner());
            match broker.create_topic(&name) {
                Ok(()) => ResponseKind::Ok(SuccessBody {
                    data: SuccessType::CreateTopic { name },
                }),
                Err(e) => ResponseKind::Err(ResponseError { message: e.to_string() }),
            }
        }
        Action::ListTopics => {
            let broker = broker.lock().unwrap_or_else(|e| e.into_inner());
            let topics = broker.list_topics();
            ResponseKind::Ok(SuccessBody {
                data: SuccessType::ListTopics { topics },
            })
        }
        Action::DeleteTopic { name } => {
            let mut broker = broker.lock().unwrap_or_else(|e| e.into_inner());
            match broker.delete_topic(&name) {
                Ok(()) => ResponseKind::Ok(SuccessBody {
                    data: SuccessType::DeleteTopic { name },
                }),
                Err(e) => ResponseKind::Err(ResponseError { message: e.to_string() }),
            }
        }
        Action::Seek { topic, consumer_id, offset } => {
            let mut broker = broker.lock().unwrap_or_else(|e| e.into_inner());
            match broker.seek(&topic, &consumer_id, offset) {
                Ok(offset) => ResponseKind::Ok(SuccessBody {
                    data: SuccessType::Seek { offset },
                }),
                Err(e) => ResponseKind::Err(ResponseError { message: e.to_string() }),
            }
        }
    }
}
