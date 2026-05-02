use crate::store::log::Message;
use anyhow::{Result, anyhow};
use std::fs::DirBuilder;
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
pub struct ReadResult {
    pub messages: Vec<Message>,
    pub next_offset: u64,
}

impl Broker {
    pub fn try_new(broker_path: impl AsRef<Path>) -> Result<Self> {
        let broker_path = broker_path.as_ref().to_path_buf();
        let topics_path = broker_path.join("topics");
        DirBuilder::new().recursive(true).create(&topics_path)?;

        let topics = Topics::new(&topics_path);
        let offsets = Offsets::new(broker_path.join("offsets"))?;

        Ok(Self { topics, offsets, broker_path })
    }

    pub fn create_topic(&mut self, name: &str) -> Result<()> {
        self.topics.create(name)
    }

    pub fn list_topics(&self) -> Vec<String> {
        self.topics.list()
    }

    pub fn delete_topic(&mut self, name: &str) -> Result<()> {
        self.topics.delete(name)
    }

    pub fn append(&mut self, topic: &str, payload: &[u8]) -> Result<u64> {
        self.topics.append(topic, payload)
    }

    pub fn read_from(&self, topic: &str, consumer_id: &str) -> Result<ReadResult> {
        let latest = self.topics.latest_offset(topic)
            .ok_or_else(|| anyhow!("topic '{}' does not exist", topic))?;
        let offset = self.offsets.get_offset(topic, consumer_id).unwrap_or(latest);
        let messages = self.topics.read_from(topic, offset).unwrap_or_default();
        let next_offset = messages.last().map(|m| m.offset + 1).unwrap_or(latest);
        Ok(ReadResult { messages, next_offset })
    }

    pub fn seek(&mut self, topic: &str, consumer_id: &str, offset: u64) -> Result<u64> {
        let latest = self.topics.latest_offset(topic)
            .ok_or_else(|| anyhow!("topic '{}' does not exist", topic))?;
        if offset > latest {
            return Err(anyhow!("offset {} is out of range, topic '{}' has {} messages", offset, topic, latest));
        }
        self.offsets.commit_offset(topic, consumer_id, offset)
    }

    pub fn commit_offset(&mut self, topic: &str, consumer_id: &str, offset: u64) -> Result<u64> {
        self.offsets.commit_offset(topic, consumer_id, offset)
    }
}
