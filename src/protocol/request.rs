use anyhow::Result as AnyhowResult;
use serde::{Deserialize};

#[derive(Debug, Deserialize)]
pub struct Request {
    pub action: Action,
}

pub fn decode_request(req: &str) -> AnyhowResult<Request> {
    Ok(serde_json::from_str(req)?)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename = "snake_case")]
pub enum Action {
    Write { topic: String, message: Vec<u8> },
    Read { topic: String, offset: u64 },
}