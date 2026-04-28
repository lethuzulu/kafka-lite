use crate::queue::Message;
use anyhow::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ResponseKind {
    Ok(SuccessBody),
    Err(ResponseError),
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SuccessType {
    Write { offset: u64 },
    Read { messages: Vec<Message> },
}

#[derive(Debug, Serialize)]
pub struct SuccessBody {
    pub data: SuccessType,
}

#[derive(Debug, Serialize)]
pub struct ResponseError {
    pub message: String,
}

pub fn encode_response(res: ResponseKind) -> Result<String> {
    Ok(serde_json::to_string(&res)?)
}
