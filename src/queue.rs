#[derive(Debug)]
pub struct MessageQueue {
    log: Vec<Message>,
    next_offset: u64,
}

#[derive(Debug, Clone)]
pub struct Message {
    payload: Vec<u8>,
    offset: u64,
}

impl MessageQueue {
    pub fn new() -> Self {
        let log = Vec::new();
        let next_offset = 0;
        Self { log, next_offset }
    }

    pub fn append(&mut self, payload: Vec<u8>) -> u64 {
        let offset = self.next_offset;
        let message = Message { payload, offset };

        self.log.push(message);
        self.next_offset += 1;
        offset
    }

    pub fn read_from(&self, offset: u64) -> Vec<Message> {
        let mut messages = Vec::new();

        if offset as usize > self.log.len() {
            return messages;
        }

        for msg in &self.log[offset as usize..] {
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
        let mut queue = MessageQueue::new();

        let o1 = queue.append(vec![1]);
        let o2 = queue.append(vec![2]);

        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
    }

    #[test]
    fn read_from_returns_correct_slice() {
        let mut queue = MessageQueue::new();

        queue.append(vec![1]);
        queue.append(vec![2]);
        queue.append(vec![3]);

        let msgs = queue.read_from(1);

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 1);
        assert_eq!(msgs[1].offset, 2);
    }

    #[test]
    fn read_from_future_offset_returns_empty() {
        let mut queue = MessageQueue::new();

        queue.append(vec![1]);

        let msgs = queue.read_from(10);

        assert!(msgs.is_empty());
    }
}
