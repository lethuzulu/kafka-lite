use anyhow::Result;
use serde::Serialize;
use crate::store::log::Message;

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ResponseKind {
    Ok(SuccessBody),
    Err(ResponseError),
}

#[derive(Debug, Serialize)]
pub struct SuccessBody {
    pub data: SuccessType,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SuccessType {
    Write { offset: u64 },
    Read { messages: Vec<Message>, next_offset: u64 },
    Commit { offset: u64}
}


#[derive(Debug, Serialize)]
pub struct ResponseError {
    pub message: String,
}

pub fn encode_response(res: ResponseKind) -> Result<String> {
    let mut res = serde_json::to_string(&res)?;
    res.push('\n');
    Ok(res)
}
