use std::collections::HashMap;
use std::fs::{read_dir, remove_file};
use std::path::{Path, PathBuf};
use anyhow::{anyhow, Result};
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
            eprintln!("failed to rebuild topics {}", e);
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
