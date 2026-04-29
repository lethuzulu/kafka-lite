use crate::store::log::{Log, Message};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::fs::read_dir;
use std::fs::{DirBuilder, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
struct OffsetEntry {
    topic: String,
    consumer_id: String,
    offset: u64,
}
#[derive(Debug)]
pub struct Broker {
    topics: HashMap<String, Log>,
    offsets: HashMap<(String, String), u64>,
    path: PathBuf,
    offset_file: File,
}

impl Broker {
    pub fn try_new(path: impl AsRef<Path>, offset_path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        DirBuilder::new().recursive(true).create(&path)?;

        let offsets = Self::replay_offsets(&offset_path)?;
        let topics = Self::replay_broker(&path)?;

        let offset_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(offset_path)?;
        Ok(Self {
            topics,
            offsets,
            path,
            offset_file,
        })
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

    pub fn read_from(&self, topic: &str, consumer_id: &str) -> (Vec<Message>, u64) {
        let offset = match self
            .offsets
            .get(&(topic.to_string(), consumer_id.to_string()))
        {
            Some(v) => v.clone(),
            None => 0, // the consumer is new, offset from the beginning
        };

        let messages = match self.topics.get(topic) {
            Some(log) => log.read_from(offset),
            None => Vec::new(),
        };
        let next_offset = offset + messages.len() as u64;
        (messages, next_offset)
    }

    pub fn commit_offset(&mut self, topic: &str, consumer_id: &str, offset: u64) -> u64 {
        self.append_offset_file(topic, consumer_id, offset);
        self.offsets
            .insert((topic.to_owned(), consumer_id.to_owned()), offset);
        offset
    }

    fn append_offset_file(&mut self, topic: &str, consumer_id: &str, offset: u64) -> Result<()> {
        let offset_entry = OffsetEntry {
            topic: topic.to_string(),
            consumer_id: consumer_id.to_string(),
            offset,
        };
        let mut offset_entry = serde_json::to_string(&offset_entry)?;
        offset_entry.push('\n');

        self.offset_file.write_all(offset_entry.as_bytes())?;
        Ok(())
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

    fn replay_offsets(path: impl AsRef<Path>) -> Result<HashMap<(String, String), u64>> {
        let mut offsets = HashMap::new();

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut buf = String::new();

        file.read_to_string(&mut buf)?;

        for line in buf.lines() {
            let offset_entry = serde_json::from_str::<OffsetEntry>(line)?;
            offsets.insert(
                (offset_entry.topic, offset_entry.consumer_id),
                offset_entry.offset,
            );
        }
        Ok(offsets)
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
