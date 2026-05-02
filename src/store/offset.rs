use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::io::{BufRead, Read, Write};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use log::error;
use std::fs::File;

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
struct OffsetKey {
    topic: String,
    consumer_id: String,

}

#[derive(Debug, Serialize, Deserialize)]
struct OffsetEntry {
    key: OffsetKey,
    value: u64
}

#[derive(Debug)]
pub struct Offsets {
    pub offsets: HashMap<OffsetKey, u64>,
    pub write_handle: File
}

impl Offsets {

    pub fn new(path: impl AsRef<Path>) -> Result<Self> {

        let write_handle = OpenOptions::new().append(true).create(true).open(&path)?;
        let offsets = Self::replay_offsets(path).unwrap_or_else(|e| {
            error!("failed to rebuild offsets {}", e);
            HashMap::<OffsetKey, u64>::new()
        });

        Ok(Self{offsets, write_handle})
    }
    pub fn commit_offset(&mut self, topic: &str, consumer_id: &str, offset: u64) -> Result<u64> {
        let offset_key = OffsetKey {topic: topic.into(), consumer_id: consumer_id.into()};
        let offset_entry = OffsetEntry { key: offset_key.clone(), value: offset };

        self.append_offset_file(&offset_entry)?;

        self.offsets.insert(offset_key, offset);
        Ok(offset)
    }

    pub fn get_offset(&self, topic: impl Into<String>, consumer_id: impl Into<String>) -> Option<u64> {
        let topic = topic.into();
        let consumer_id = consumer_id.into();
        let offset_key = OffsetKey {topic, consumer_id};
        let offset = self.offsets.get(&offset_key).cloned();
        offset
    }

    fn replay_offsets(path : impl AsRef<Path>) -> Result<HashMap<OffsetKey, u64>>{
        let mut offset_file = OpenOptions::new().read(true).open(path)?;

        let mut offsets = HashMap::new();

        let mut buf = String::new();

        offset_file.read_to_string(&mut buf)?;

        for line in buf.lines() {
            let offset_entry = serde_json::from_str::<OffsetEntry>(line)?;
            let key = offset_entry.key;
            let value = offset_entry.value;
            offsets.insert(key, value);
        }
        Ok(offsets)
    }
    
    fn append_offset_file(&mut self, offset_entry: &OffsetEntry) -> Result<()> {
        let mut offset_entry = serde_json::to_string(offset_entry)?;
        offset_entry.push('\n');
        self.write_handle.write_all(offset_entry.as_bytes())?;
        self.write_handle.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn commit_and_get_offset() {
        let file = NamedTempFile::new().unwrap();
        let mut offsets = Offsets::new(file.path()).unwrap();

        offsets.commit_offset("orders", "svc-a", 5).unwrap();

        assert_eq!(offsets.get_offset("orders", "svc-a"), Some(5));
    }

    #[test]
    fn get_offset_returns_none_for_unknown_consumer() {
        let file = NamedTempFile::new().unwrap();
        let offsets = Offsets::new(file.path()).unwrap();

        assert_eq!(offsets.get_offset("orders", "svc-unknown"), None);
    }

    #[test]
    fn commit_offset_overwrites_previous() {
        let file = NamedTempFile::new().unwrap();
        let mut offsets = Offsets::new(file.path()).unwrap();

        offsets.commit_offset("orders", "svc-a", 3).unwrap();
        offsets.commit_offset("orders", "svc-a", 7).unwrap();

        assert_eq!(offsets.get_offset("orders", "svc-a"), Some(7));
    }

    #[test]
    fn offsets_are_isolated_per_consumer() {
        let file = NamedTempFile::new().unwrap();
        let mut offsets = Offsets::new(file.path()).unwrap();

        offsets.commit_offset("orders", "svc-a", 10).unwrap();
        offsets.commit_offset("orders", "svc-b", 2).unwrap();

        assert_eq!(offsets.get_offset("orders", "svc-a"), Some(10));
        assert_eq!(offsets.get_offset("orders", "svc-b"), Some(2));
    }

    #[test]
    fn offsets_survive_restart() {
        let file = NamedTempFile::new().unwrap();

        {
            let mut offsets = Offsets::new(file.path()).unwrap();
            offsets.commit_offset("orders", "svc-a", 4).unwrap();
            offsets.commit_offset("payments", "svc-b", 9).unwrap();
        }

        let offsets = Offsets::new(file.path()).unwrap();
        assert_eq!(offsets.get_offset("orders", "svc-a"), Some(4));
        assert_eq!(offsets.get_offset("payments", "svc-b"), Some(9));
    }

    #[test]
    fn latest_commit_wins_on_replay() {
        let file = NamedTempFile::new().unwrap();

        {
            let mut offsets = Offsets::new(file.path()).unwrap();
            offsets.commit_offset("orders", "svc-a", 1).unwrap();
            offsets.commit_offset("orders", "svc-a", 5).unwrap();
            offsets.commit_offset("orders", "svc-a", 3).unwrap();
        }

        let offsets = Offsets::new(file.path()).unwrap();
        // last written entry wins on replay (last-write-wins)
        assert_eq!(offsets.get_offset("orders", "svc-a"), Some(3));
    }
}