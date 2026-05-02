use crate::store::log::{ Message};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs::{DirBuilder};
use std::path::{Path, PathBuf};
use crate::store::offset::Offsets;
use crate::store::topic::Topics;

#[derive(Debug)]
pub struct Broker {
    topics: Topics,
    offsets: Offsets,
    broker_path: PathBuf,
}

#[derive(Debug)]
pub struct ReadResult{
    pub messages: Vec<Message>,
    pub next_offset: u64
}

impl Broker {
    pub fn try_new(broker_path: impl AsRef<Path>) -> Result<Self> {
        let broker_path = broker_path.as_ref().to_path_buf();
        DirBuilder::new().recursive(true).create(&broker_path)?;

        let topics = Topics::new(&broker_path);

        let offsets = Offsets::new(broker_path.join("offsets"))?;

        Ok(Self {
            topics,
            offsets,
            broker_path,
        })
    }

    pub fn append(&mut self, topic: &str, payload: &[u8]) -> Result<u64> {
        let offset = self.topics.append(topic, payload);
        offset
    }

    pub fn read_from(&self, topic: &str, consumer_id: &str) -> Result<ReadResult>{

        let offset = self.offsets.get_offset(topic, consumer_id).unwrap_or( 0);
        let messages = self.topics.read_from(topic, offset);
        match messages {
            Some(messages) => {
                let next_offset = messages.last().map(|m| m.offset + 1).unwrap_or(0);
                let read_result = ReadResult {messages, next_offset};
                Ok(read_result)
            },
            None => Err(anyhow!("unknown topic {} or unknown consumer_id {}", topic, consumer_id))
        }
    }

    pub fn commit_offset(&mut self, topic: &str, consumer_id: &str, offset: u64) -> u64 {
        self.offsets.commit_offset(topic, consumer_id, offset)
    }


    fn list_topics() {}
    pub fn create_topic() {}

    pub fn delete_topic() {}

}
