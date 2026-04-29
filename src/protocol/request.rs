use anyhow::Result as AnyhowResult;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Request {
    pub action: Action,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Write {
        topic: String,
        message: Vec<u8>,
    },
    Read {
        topic: String,
        consumer_id: String,
    },
    Commit {
        topic: String,
        consumer_id: String,
        offset: u64,
    },
}

pub fn decode_request(req: &str) -> AnyhowResult<Request> {
    Ok(serde_json::from_str(req)?)
}
