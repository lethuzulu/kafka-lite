use crate::protocol::request::{Action, Request, decode_request};
use crate::protocol::response::{
    ResponseError, ResponseKind, SuccessBody, SuccessType, encode_response,
};
use anyhow::Result;
use std::io::BufRead;
use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use crate::store::broker::Broker;

#[derive(Debug)]
pub struct TcpServer {
    inner: TcpListener,
    broker: Arc<Mutex<Broker>>,
}

impl TcpServer {
    pub fn try_new(address: impl ToSocketAddrs) -> Result<Self> {
        let inner = TcpListener::bind(address)?;

        let broker = Broker::try_new("data")?;
        let broker = Arc::new(Mutex::new(broker));

        println!("Tcp started...");

        Ok(Self { inner, broker })
    }

    pub fn listen(&self) {
        for conn in self.inner.incoming() {
            match conn {
                Ok(stream) => {
                    let broker = Arc::clone(&self.broker);
                    thread::spawn(|| handle_connection(stream, broker));
                }
                Err(_) => println!("error occurred. listening for next connection."),
            }
        }
    }
}

fn handle_connection(stream: TcpStream, broker: Arc<Mutex<Broker>>) {
    let mut line = String::new();

    let mut writer = stream.try_clone().unwrap();
    let mut buf_reader = BufReader::new(stream);

    loop {
        match buf_reader.read_line(&mut line) {
            Ok(0) => break, //client disconnected
            Ok(_) => {}
            Err(e) => {
                eprintln!("read error: {}", e);
                break;
            }
        }
        let request = match decode_request(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("error, failed to deserialize request: {}", e);

                let err = ResponseKind::Err(ResponseError {message : "bad request".to_string()});

                let res = match encode_response(err) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("failure to serialize the response {}", e);
                        break
                    }
                };
                if let Err(e) =writer.write_all(res.as_bytes()) {
                    eprintln!("failure to write to socket")
                }
                line.clear();
                continue;
            }
        };

        let result = handle_request(request, &broker);
        let response = match encode_response(result) {
            Ok(response) => response,
            Err(e) => {
                eprintln!("failed to encode response: {}", e);
                break;
            }
        };
        if let Err(e) = writer.write_all(response.as_bytes()) {
            eprintln!("failed to write the socker {}", e);
        }
        line.clear()
    }
}

fn handle_request(req: Request, broker: &Arc<Mutex<Broker>>) -> ResponseKind {
    let mut broker = broker.lock().unwrap();
    match req.action {
        Action::Write { topic, message } => match broker.append(topic, message) {
            Ok(offset) => ResponseKind::Ok(SuccessBody {
                data: SuccessType::Write { offset },
            }),
            Err(_) => ResponseKind::Err(ResponseError {
                message: "internal error".into(),
            }),
        },
        Action::Read { topic, consumer_id } => {
            let (messages, next_offset) = broker.read_from(&topic, &consumer_id);
            ResponseKind::Ok(SuccessBody {
                data: SuccessType::Read { messages, next_offset },
            })
        },
        Action::Commit {topic, consumer_id, offset} => {
            let  offset = broker.commit_offset(&topic, &consumer_id, offset);
            ResponseKind::Ok(SuccessBody {
                data: SuccessType::Commit {offset}
            })
        }
    }
}
