use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::DirBuilder;
use std::fs::{File, OpenOptions, create_dir_all, read_dir};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    payload: Vec<u8>,
    offset: u64,
}

#[derive(Debug)]
pub struct Broker {
    topics: HashMap<String, Log>,
    path: PathBuf,
}

#[derive(Debug)]
pub struct Log {
    messages: Vec<Message>,
    next_offset: u64,
    file: File,
}

impl Broker {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let dir = DirBuilder::new().recursive(true).create(&path)?;

        let topics = Self::replay_broker(&path)?;

        Ok(Self { topics, path })
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

    pub fn read_from(&self, topic: &str, offset: u64) -> Vec<Message> {
        let messages = match self.topics.get(topic) {
            Some(log) => log.read_from(offset),
            None => Vec::new(),
        };
        messages
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

impl Log {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let messages = match Self::replay_log(&path) {
            Ok(log) => log,
            Err(_) => Vec::new(), // TODO too aggressive, discriminate against certain errors
        };

        let next_offset = match messages.last() {
            Some(m) => m.offset + 1,
            None => 0,
        };

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            messages,
            next_offset,
            file,
        })
    }

    pub fn append(&mut self, payload: Vec<u8>) -> Result<u64> {
        let offset = self.next_offset;
        let message = Message { payload, offset };

        self.append_log_file(&message)?;
        self.messages.push(message);
        self.next_offset += 1;
        Ok(offset)
    }

    pub fn read_from(&self, offset: u64) -> Vec<Message> {
        let mut messages = Vec::new();

        if offset as usize >= self.messages.len() {
            return messages;
        }

        for msg in &self.messages[offset as usize..] {
            let msg = msg.clone();
            messages.push(msg);
        }
        messages
    }

    pub fn append_log_file(&mut self, line: &Message) -> Result<()> {
        let mut msg_string = serde_json::to_string(line)?;
        msg_string.push('\n');
        self.file.write_all(msg_string.as_bytes())?;
        Ok(())
    }

    pub fn replay_log(path: impl AsRef<Path>) -> Result<Vec<Message>> {
        let mut log = OpenOptions::new().read(true).open(path)?;

        let mut messages = Vec::new();
        let mut buf = String::new();

        log.read_to_string(&mut buf)?;

        for line in buf.lines() {
            let message = serde_json::from_str::<Message>(line)?;
            messages.push(message)
        }
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn append_increments_offset() {
        let file = NamedTempFile::new().unwrap();
        let mut log = Log::try_new(file.path()).unwrap();

        let o1 = log.append(vec![1]).unwrap();
        let o2 = log.append(vec![2]).unwrap();

        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
    }

    #[test]
    fn read_from_returns_correct_slice() {
        let file = NamedTempFile::new().unwrap();
        let mut log = Log::try_new(file.path()).unwrap();

        log.append(vec![1]).unwrap();
        log.append(vec![2]).unwrap();
        log.append(vec![3]).unwrap();

        let msgs = log.read_from(1);

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[1].offset, 2);
    }

    #[test]
    fn read_from_future_offset_returns_empty() {
        let file = NamedTempFile::new().unwrap();
        let mut log = Log::try_new(file.path()).unwrap();

        log.append(vec![1]).unwrap();

        let msgs = log.read_from(10);

        assert!(msgs.is_empty());
    }

    #[test]
    fn broker_recovers_topics_after_restart() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();

        {
            let mut broker = Broker::try_new(dir.path()).unwrap();
            broker.append("orders", b"msg1".to_vec()).unwrap();
            broker.append("orders", b"msg2".to_vec()).unwrap();
        }

        // simulate restart
        let broker = Broker::try_new(dir.path()).unwrap();

        let msgs = broker.read_from("orders", 0);

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 0);
        assert_eq!(msgs[1].offset, 1);
    }
}
