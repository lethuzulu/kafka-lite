use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::io::{BufRead, Read, Write};
use serde::{Deserialize, Serialize};
use anyhow::Result;
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
            eprintln!("failed to rebuild offsets {}", e);
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