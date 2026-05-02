use std::collections::HashMap;
use std::fs::{read_dir, remove_file};
use std::path::{Path, PathBuf};
use anyhow::{anyhow, Result};
use log::error;
use crate::store::log::{Log, Message};


#[derive(Debug)]
pub struct Topics {
    logs: HashMap<String, Log>,
    path: PathBuf,
}

impl Topics {

    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let logs = Self::replay_topics(&path).unwrap_or_else(|e| {
            error!("failed to rebuild topics {}", e);
            HashMap::new()
        });

        Self { logs, path }
    }

    pub fn create(&mut self, name: &str) -> Result<()> {
        if self.logs.contains_key(name) {
            return Err(anyhow!("topic '{}' already exists", name));
        }
        let log = Log::try_new(self.path.join(name))?;
        self.logs.insert(name.to_string(), log);
        Ok(())
    }

    pub fn list(&self) -> Vec<String> {
        self.logs.keys().cloned().collect()
    }

    pub fn delete(&mut self, name: &str) -> Result<()> {
        remove_file(self.path.join(name))?;
        if self.logs.remove(name).is_none() {
            return Err(anyhow!("topic '{}' does not exist", name));
        }
        Ok(())
    }

    pub fn append(&mut self, topic: &str, payload: &[u8]) -> Result<u64> {
        let log = self.logs.get_mut(topic).ok_or_else(|| anyhow!("topic '{}' does not exist", topic))?;
        let offset = log.append(payload)?;
        Ok(offset)
    }

    pub fn latest_offset(&self, topic: &str) -> Option<u64> {
        self.logs.get(topic).map(|log| log.next_offset)
    }

    pub fn read_from(&self, topic: &str, offset: u64) -> Option<Vec<Message>> {
        match self.logs.get(topic) {
            Some(log) => Some(log.read_from(offset)),
            None => None,
        }
    }

    fn replay_topics(path: impl AsRef<Path>) -> Result<HashMap<String, Log>> {
        let topic_dir = read_dir(path)?;

        let mut topics = HashMap::new();

        for entry in topic_dir {
            let topic = entry?;
            if !topic.file_type()?.is_file() {
                continue;
            }
            let topic_path = topic.path();
            let topic_name = topic.file_name();
            let topic = topic_name.into_string().map_err(|_| anyhow!("invalid topic name"))?;
            let log = Log::try_new(topic_path)?;
            topics.insert(topic, log);
        }
        Ok(topics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_topic_and_append() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        topics.create("orders").unwrap();
        let offset = topics.append("orders", b"hello").unwrap();

        assert_eq!(offset, 0);
    }

    #[test]
    fn create_duplicate_topic_returns_error() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        topics.create("orders").unwrap();
        assert!(topics.create("orders").is_err());
    }

    #[test]
    fn append_to_nonexistent_topic_returns_error() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        assert!(topics.append("ghost", b"data").is_err());
    }

    #[test]
    fn list_topics_returns_all_created() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        topics.create("orders").unwrap();
        topics.create("payments").unwrap();

        let mut list = topics.list();
        list.sort();
        assert_eq!(list, vec!["orders", "payments"]);
    }

    #[test]
    fn delete_topic_removes_it() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        topics.create("orders").unwrap();
        topics.delete("orders").unwrap();

        assert!(topics.list().is_empty());
    }

    #[test]
    fn latest_offset_reflects_appends() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        topics.create("orders").unwrap();
        assert_eq!(topics.latest_offset("orders"), Some(0));

        topics.append("orders", b"msg1").unwrap();
        assert_eq!(topics.latest_offset("orders"), Some(1));

        topics.append("orders", b"msg2").unwrap();
        assert_eq!(topics.latest_offset("orders"), Some(2));
    }

    #[test]
    fn read_from_returns_correct_messages() {
        let dir = tempdir().unwrap();
        let mut topics = Topics::new(dir.path());

        topics.create("orders").unwrap();
        topics.append("orders", b"first").unwrap();
        topics.append("orders", b"second").unwrap();
        topics.append("orders", b"third").unwrap();

        let msgs = topics.read_from("orders", 1).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[1].offset, 2);
    }

    #[test]
    fn topics_replay_on_restart() {
        let dir = tempdir().unwrap();

        {
            let mut topics = Topics::new(dir.path());
            topics.create("orders").unwrap();
            topics.append("orders", b"msg1").unwrap();
            topics.append("orders", b"msg2").unwrap();
        }

        let topics = Topics::new(dir.path());
        let msgs = topics.read_from("orders", 0).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 0);
        assert_eq!(msgs[1].offset, 1);
    }
}
