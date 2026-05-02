use std::collections::HashMap;
use std::fs::read_dir;
use std::path::Path;
use anyhow::{anyhow, Result};
use crate::store::log::{Log, Message};


#[derive(Debug)]
pub struct Topics (HashMap<String, Log>);

impl Topics {

    pub fn new(path: impl AsRef<Path>) -> Self {
        let topics = match Self::replay_topics(path) {
            Ok(topics) => topics,
            Err(e) => {
                eprintln!("failed to rebuild topics {}", e);
                HashMap::<String, Log>::new()
            }
        };

        Self(topics)
    }

    pub fn append(&mut self, topic: &str, payload: &[u8]) -> Result<u64>{
        let log = self.0.get_mut(topic).ok_or_else(|| anyhow!("topic '{}' does not exist", topic))?;
        let offset = log.append(payload)?;
        Ok(offset)
    }

    pub fn read_from(&self, topic: &str, offset: u64) -> Option<Vec<Message>>{
        match self.0.get(topic){
            Some(log) => {
                let messages = log.read_from(offset);
                Some(messages)
            },
            None => None
        }
    }

    fn replay_topics(path: impl AsRef<Path>) -> Result<HashMap<String, Log>> {
        let topic_dir = read_dir(path)?;

        let mut topics = HashMap::new();

        for entry in topic_dir {
            let topic = entry?;
            let topic_path = topic.path();
            let topic_name = topic.file_name();
            let topic = topic_name.into_string().map_err(|_| anyhow!("invalid topic name"))?;
            let log = Log::try_new(topic_path)?;
            topics.insert(topic, log);
        }
        Ok(topics)
    }

    fn list() {}
    fn create(){}
    fn delete(){}
}