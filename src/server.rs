use crate::protocol::request::{Action, Request, decode_request};
use crate::protocol::response::{
    ResponseError, ResponseKind, SuccessBody, SuccessType, encode_response,
};
use crate::queue::Broker;
use anyhow::Result;
use std::io::BufRead;
use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

#[derive(Debug)]
pub struct TcpServer {
    inner: TcpListener,
    broker: Broker,
}

impl TcpServer {
    pub fn try_new(address: impl ToSocketAddrs) -> Result<Self> {
        let inner = TcpListener::bind(address)?;

        let broker = Broker::try_new("data")?;

        println!("Tcp started...");

        Ok(Self { inner, broker })
    }

    pub fn listen(&mut self) {
        for conn in self.inner.incoming() {
            match conn {
                Ok(stream) => handle_connection(stream, &mut self.broker),
                Err(_) => println!("error occurred. listening for next connection."),
            }
        }
    }
}

fn handle_connection(stream: TcpStream, broker: &mut Broker) {
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
            Err(_) => {
                eprintln!("error, failed to deserialize request");
                continue;
            }
        };

        let result = handle_request(request, broker);
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

fn handle_request(req: Request, broker: &mut Broker) -> ResponseKind {
    match req.action {
        Action::Write { topic, message } => match broker.append(topic, message) {
            Ok(offset) => ResponseKind::Ok(SuccessBody {
                data: SuccessType::Write { offset },
            }),
            Err(_) => ResponseKind::Err(ResponseError {
                message: "internal error".into(),
            }),
        },
        Action::Read { topic, offset } => {
            let messages = broker.read_from(&topic, offset);
            ResponseKind::Ok(SuccessBody {
                data: SuccessType::Read { messages },
            })
        }
    }
}
