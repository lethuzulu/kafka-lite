use anyhow::{Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path};

pub use kafka_lite_protocol::Message;
#[derive(Debug)]
pub struct Log {
    pub messages: Vec<Message>,
    pub next_offset: u64,    // this is the offset used for writing new messages, not the offset used by consumers for reading
    pub write_handler: File,
}

impl Log {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let write_handler = OpenOptions::new().create(true).append(true).open(&path)?; //cache the write handler since we call it often
        
        let messages = match Self::replay_log(&path) {
            Ok(log) => log,
            Err(_) => Vec::new(), // TODO too aggressive, discriminate against certain errors
        };

        let next_offset = match messages.last() {
            Some(m) => m.offset + 1,
            None => 0,
        };

        Ok(Self {
            messages,
            next_offset,
            write_handler,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        let offset = self.next_offset;
        let message = Message { payload: payload.to_vec(), offset };

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
        self.write_handler.write_all(msg_string.as_bytes())?;
        self.write_handler.flush()?;
        Ok(())
    }

    pub fn replay_log(path: impl AsRef<Path>) -> Result<Vec<Message>> {
        let mut log_file = OpenOptions::new().read(true).open(path)?; // always create a new replay read handler since we replay once at system startup.

        let mut messages = Vec::new();
        let mut buf = String::new();

        log_file.read_to_string(&mut buf)?;

        for line in buf.lines() {
            let message = serde_json::from_str::<Message>(line)?;
            messages.push(message)
        }
        Ok(messages)
    }
}
// 
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tempfile::NamedTempFile;
// 
//     #[test]
//     fn append_increments_offset() {
//         let file = NamedTempFile::new().unwrap();
//         let mut log = Log::try_new(file.path()).unwrap();
// 
//         let o1 = log.append(vec![1]).unwrap();
//         let o2 = log.append(vec![2]).unwrap();
// 
//         assert_eq!(o1, 0);
//         assert_eq!(o2, 1);
//     }
// 
//     #[test]
//     fn read_from_returns_correct_slice() {
//         let file = NamedTempFile::new().unwrap();
//         let mut log = Log::try_new(file.path()).unwrap();
// 
//         log.append(vec![1]).unwrap();
//         log.append(vec![2]).unwrap();
//         log.append(vec![3]).unwrap();
// 
//         let msgs = log.read_from(1);
// 
//         assert_eq!(msgs.len(), 2);
//         assert_eq!(msgs[0].offset, 1);
//         assert_eq!(msgs[1].offset, 2);
//     }
// 
//     #[test]
//     fn read_from_future_offset_returns_empty() {
//         let file = NamedTempFile::new().unwrap();
//         let mut log = Log::try_new(file.path()).unwrap();
// 
//         log.append(vec![1]).unwrap();
// 
//         let msgs = log.read_from(10);
// 
//         assert!(msgs.is_empty());
//     }
// }
