use crate::arbiter::{self, Arbiter};
use crate::messages;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedReceiver;

pub type SID = u64;
pub type Result<T> = std::result::Result<T, Error>;
pub type TID = (SID, u64);
pub type RID = String;

pub struct Coordinator {
    next_sid: SID,
    arbiter: Arbiter<TID, RID>,
    sessions: HashMap<SID, Session>
}

impl Coordinator {
    pub fn new(arbiter: Arbiter<TID, RID>) -> Self {
        Coordinator {
            next_sid: 0,
            arbiter,
            sessions: HashMap::new()
        }
    }

    pub fn start_session(&mut self, queue: UnboundedReceiver<messages::OutgoingMessage>) -> SID {
        let sid = self.next_sid;
        self.next_sid = self.next_sid + 1;

        let session = Session {
            id: sid,
            queue
        };
        self.sessions.insert(sid, session);

        sid
    }

    pub fn handle_message(&mut self, sid: SID, message: messages::IncomingMessage) {
        let res = message.message
            .ok_or(Error::MalformedRequest(String::from("message was not specified")))
            .and_then(|message| {
                match message {
                    messages::incoming_message::Message::StartTransaction(message) => self.start_transaction(sid, message),
                    messages::incoming_message::Message::ResourcesAccessed(message) => self.resources_accessed(sid, message)
                }
            });
    }

    fn start_transaction(&mut self, sid: SID, message: messages::StartTransaction) -> Result<()> {
        let tid = (sid, message.tid);
        self.arbiter.start_transaction(tid);
        Ok(())
    }

    fn resources_accessed(&mut self, sid: SID, message: messages::ResourcesAccessed) -> Result<()> {
        let tid = (sid, message.tid);
        let res = self.arbiter.transaction_progress_many(&tid, message.read_rids, message.written_rids);

        self.handle_result(res);
        Ok(())
    }

    fn handle_result(&mut self, res: arbiter::Result<arbiter::TransactionUpdate<TID>, TID>) {

    }
}

struct Session {
    id: SID,
    queue: UnboundedReceiver<messages::OutgoingMessage>
}

pub enum Error {
    MalformedRequest(String)
}