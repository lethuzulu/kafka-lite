use std::collections::HashMap;

#[derive(Debug)]
pub struct Broker {
    topics: HashMap<String, Log>,
}

#[derive(Debug)]
pub struct Log {
    messages: Vec<Message>,
    next_offset: u64,
}

#[derive(Debug, Clone)]
pub struct Message {
    payload: Vec<u8>,
    offset: u64,
}

impl Broker {
    pub fn new() -> Self {
        let topics = HashMap::new();
        Self { topics }
    }

    pub fn append(&mut self, topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> u64 {
        let topic = topic.into();
        let payload = payload.into();

        if !self.topics.contains_key(&topic) {
            // if the topic does not exist, create it and insert a new log
            let mut log = Log::new();
            let offset = log.append(payload);

            self.topics.insert(topic, log).unwrap();
            return offset;
        }

        let log = self.topics.get_mut(&topic).unwrap(); // use Entry API later
        let offset = log.append(payload);
        offset
    }

    pub fn read_from(&self, topic: &str, offset: u64) -> Vec<Message>{
        let messages = match self.topics.get(topic){
            Some(log) => log.read_from(offset),
            None => Vec::new()
        };
        messages
    }
}

impl Log {
    pub fn new() -> Self {
        let log = Vec::new();
        let next_offset = 0;
        Self {
            messages: log,
            next_offset,
        }
    }

    pub fn append(&mut self, payload: Vec<u8>) -> u64 {
        let offset = self.next_offset;
        let message = Message { payload, offset };

        self.messages.push(message);
        self.next_offset += 1;
        offset
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_increments_offset() {
        let mut log = Log::new();

        let o1 = log.append(vec![1]);
        let o2 = log.append(vec![2]);

        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
    }

    #[test]
    fn read_from_returns_correct_slice() {
        let mut log = Log::new();

        log.append(vec![1]);
        log.append(vec![2]);
        log.append(vec![3]);

        let msgs = log.read_from(1);

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[1].offset, 2);
    }

    #[test]
    fn read_from_future_offset_returns_empty() {
        let mut log = Log::new();

        log.append(vec![1]);

        let msgs = log.read_from(10);

        assert!(msgs.is_empty());
    }
}
