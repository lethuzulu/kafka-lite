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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn setup(dir: &tempfile::TempDir) -> Broker {
        Broker::try_new(dir.path()).unwrap()
    }

    #[test]
    fn create_and_append_to_topic() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        let offset = broker.append("orders", b"hello").unwrap();

        assert_eq!(offset, 0);
    }

    #[test]
    fn append_to_nonexistent_topic_errors() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        assert!(broker.append("ghost", b"data").is_err());
    }

    #[test]
    fn list_topics_returns_created_topics() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.create_topic("payments").unwrap();

        let mut list = broker.list_topics();
        list.sort();
        assert_eq!(list, vec!["orders", "payments"]);
    }

    #[test]
    fn delete_topic_removes_it() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.delete_topic("orders").unwrap();

        assert!(broker.list_topics().is_empty());
    }

    #[test]
    fn new_consumer_defaults_to_latest_offset() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.append("orders", b"msg1").unwrap();
        broker.append("orders", b"msg2").unwrap();

        // new consumer — no committed offset — should default to latest and get nothing
        let result = broker.read_from("orders", "svc-new").unwrap();
        assert!(result.messages.is_empty());
    }

    #[test]
    fn consumer_reads_from_committed_offset() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.append("orders", b"msg1").unwrap();
        broker.append("orders", b"msg2").unwrap();
        broker.append("orders", b"msg3").unwrap();

        broker.commit_offset("orders", "svc-a", 1).unwrap();

        let result = broker.read_from("orders", "svc-a").unwrap();
        assert_eq!(result.messages.len(), 2);
        assert_eq!(result.messages[0].offset, 1);
        assert_eq!(result.messages[1].offset, 2);
    }

    #[test]
    fn consumers_are_independent() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.append("orders", b"msg1").unwrap();
        broker.append("orders", b"msg2").unwrap();
        broker.append("orders", b"msg3").unwrap();

        broker.commit_offset("orders", "svc-a", 0).unwrap();
        broker.commit_offset("orders", "svc-b", 2).unwrap();

        let result_a = broker.read_from("orders", "svc-a").unwrap();
        let result_b = broker.read_from("orders", "svc-b").unwrap();

        assert_eq!(result_a.messages.len(), 3);
        assert_eq!(result_b.messages.len(), 1);
    }

    #[test]
    fn seek_changes_consumer_position() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.append("orders", b"msg1").unwrap();
        broker.append("orders", b"msg2").unwrap();
        broker.append("orders", b"msg3").unwrap();

        broker.seek("orders", "svc-a", 0).unwrap();

        let result = broker.read_from("orders", "svc-a").unwrap();
        assert_eq!(result.messages.len(), 3);
        assert_eq!(result.messages[0].offset, 0);
    }

    #[test]
    fn seek_out_of_range_returns_error() {
        let dir = tempdir().unwrap();
        let mut broker = setup(&dir);

        broker.create_topic("orders").unwrap();
        broker.append("orders", b"msg1").unwrap();

        assert!(broker.seek("orders", "svc-a", 99).is_err());
    }

    #[test]
    fn broker_recovers_topics_and_offsets_after_restart() {
        let dir = tempdir().unwrap();

        {
            let mut broker = setup(&dir);
            broker.create_topic("orders").unwrap();
            broker.append("orders", b"msg1").unwrap();
            broker.append("orders", b"msg2").unwrap();
            broker.commit_offset("orders", "svc-a", 1).unwrap();
        }

        let broker = Broker::try_new(dir.path()).unwrap();

        // messages survived restart
        let result = broker.read_from("orders", "svc-a").unwrap();
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.messages[0].offset, 1);
    }
}
