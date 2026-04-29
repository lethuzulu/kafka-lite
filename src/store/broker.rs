use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::fs::DirBuilder;
use std::fs::{read_dir};
use std::path::{Path, PathBuf};
use crate::store::log::{Log, Message};

#[derive(Debug)]
pub struct Broker {
    topics: HashMap<String, Log>,
    offsets: HashMap<(String, String), u64>,
    path: PathBuf,
}

impl Broker {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        DirBuilder::new().recursive(true).create(&path)?;

        let offsets = HashMap::new();
        let topics = Self::replay_broker(&path)?;

        Ok(Self { topics, offsets, path })
    }

    pub fn append(&mut self, topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Result<u64> {
        let topic = topic.into();
        let payload = payload.into();

        if !self.topics.contains_key(&topic) {
            let path = self.path.join(&topic);
            let mut log = Log::try_new(path)?;
            let offset = log.append(payload)?;

            self.topics.insert(topic, log);
            return Ok(offset);
        }

        let log = self.topics.get_mut(&topic).unwrap(); // use Entry API later
        let offset = log.append(payload)?;
        Ok(offset)
    }

    pub fn read_from(&self, topic: &str, consumer_id: &str) -> (Vec<Message>, u64){

        let offset = match self.offsets.get(&(topic.to_string(), consumer_id.to_string())) {
            Some(v) => v.clone(),
            None => 0   // the consumer is new, offset from the beginning
        };

        let messages = match self.topics.get(topic) {
            Some(log) => log.read_from(offset),
            None => Vec::new(),
        };
        let next_offset = offset + messages.len() as u64;
        (messages, next_offset)
    }

    pub fn commit_offset(&mut self, topic: &str, consumer_id: &str, offset:u64) -> u64 {
        self.offsets.insert((topic.to_owned(), consumer_id.to_owned()), offset);
        offset
    }

    fn replay_broker(path: impl AsRef<Path>) -> Result<HashMap<String, Log>> {
        let mut topics = HashMap::new();

        for entry in read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            let topic = entry.file_name();
            let topic = topic
                .into_string()
                .map_err(|_| anyhow!("invalid topic name"))?;
            let log = Log::try_new(&path)?;
            topics.insert(topic, log);
        }
        Ok(topics)
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::store::broker::Broker;
//
//     #[test]
//     fn broker_recovers_topics_after_restart() {
//         use tempfile::tempdir;
//
//         let dir = tempdir().unwrap();
//
//         {
//             let mut broker = Broker::try_new(dir.path()).unwrap();
//             broker.append("orders", b"msg1".to_vec()).unwrap();
//             broker.append("orders", b"msg2".to_vec()).unwrap();
//         }
//
//         // simulate restart
//         let broker = Broker::try_new(dir.path()).unwrap();
//
//         let msgs = broker.read_from("orders", 0);
//
//         assert_eq!(msgs.len(), 2);
//         assert_eq!(msgs[0].offset, 0);
//         assert_eq!(msgs[1].offset, 1);
//     }
// }
