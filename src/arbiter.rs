use linked_hash_map::LinkedHashMap;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;

type CID = u64;

pub struct Arbiter<TID, RID> {
    next_commit_id: CID,
    completed_txns: VecDeque<(CID, HashSet<RID>)>,
    pending_commits: HashSet<RID>,
    txns: LinkedHashMap<TID, Transaction<RID>>
}

impl <TID, RID> Arbiter<TID, RID>
    where TID: Eq + Hash + Clone + Debug,
          RID: Eq + Hash + Clone + Debug {
    pub fn new() -> Self {
        Arbiter {
            next_commit_id: 0,
            completed_txns: VecDeque::new(),
            pending_commits: HashSet::new(),
            txns: LinkedHashMap::new()
        }
    }

    pub fn get_state(&self, id: &TID) -> Result<TransactionState> {
        self.with_transaction(id, |txn| Ok(txn.state_type))
    }

    pub fn start_transaction(&mut self, id: TID) {
        self.txns.insert(id, Transaction::new(self.next_commit_id));
    }

    pub fn transaction_progress_many<R, W>(&mut self, id: &TID, read: R, written: W) -> Result<TransactionUpdate<TID>>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        self.with_transaction_mut(id, |txn| txn.transaction_progress_many(read, written))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_read(&mut self, id: &TID, read: RID) -> Result<TransactionUpdate<TID>> {
        self.with_transaction_mut(id, |txn| txn.transaction_progress_read(read))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_write(&mut self, id: &TID, written: RID) -> Result<TransactionUpdate<TID>> {
        self.with_transaction_mut(id, |txn| txn.transaction_progress_write(written))?;
        Ok(self.advance_txns())
    }

    pub fn start_commit(&mut self, id: &TID) -> Result<TransactionUpdate<TID>> {
        self.with_transaction_mut(id, |txn| txn.wait_commit())?;
        Ok(self.advance_txns())
    }

    pub fn commit_completed(&mut self, id: &TID) -> Result<TransactionUpdate<TID>> {
        let txn = self.txns.remove(&id).ok_or_else(|| Error::UnknownTransaction)?;
        if txn.state_type != TransactionState::Committing {
            // TODO: This changes the order
            self.txns.insert(id.clone(), txn);
            return Err(Error::InvalidTransactionState);
        }

        let commit_id = self.next_commit_id;
        self.next_commit_id += 1;
        
        for rid in txn.written.iter() {
            self.pending_commits.remove(rid);
        }
        self.completed_txns.push_back((commit_id, txn.written));
        Ok(self.advance_txns())
    }

    fn advance_txns(&mut self) -> TransactionUpdate<TID> {
        let mut result = TransactionUpdate::no_change();
        result.merge(self.advance_impossible_writes());
        result.merge(self.advance_pending_commits());

        self.remove_obsolete_commits();
        
        result
    }

    fn advance_impossible_writes(&mut self) -> TransactionUpdate<TID> {
        let mut result = TransactionUpdate::no_change();
        for (id, txn) in self.txns.iter_mut().rev() {
            match txn.state_type {
                TransactionState::InProgress |
                TransactionState::WaitCommit => {
                    for (seq, committed_writes) in self.completed_txns.iter() {
                        if *seq >= txn.created_commit && txn.is_commit_prevented(&committed_writes) {
                            result.mark_failed(id.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        for id in result.failed.iter() {
            self.txns.remove(id);
        }

        result
    }

    fn advance_pending_commits(&mut self) -> TransactionUpdate<TID> {
        let mut result = TransactionUpdate::no_change();
        for (id, txn) in self.txns.iter_mut() {
            match txn.state_type {
                TransactionState::WaitCommit => {
                    if txn.try_commit(&self.pending_commits).unwrap() {
                        self.pending_commits.extend(txn.written.iter().cloned());
                        result.mark_can_commit(id.clone());
                    }
                }
                _ => {}
            }
        }

        result
    }

    fn remove_obsolete_commits(&mut self) {
        for min_relevant in self.txns.values()
            .map(|t| t.created_commit)
            .min() {
            while self.completed_txns.get(0).map_or(false, |c| c.0 < min_relevant) {
                self.completed_txns.pop_front();
            }
        }
    }

    fn with_transaction<F, R>(&self, id: &TID, f: F) -> Result<R>
        where F: FnOnce(&Transaction<RID>) -> Result<R> {
        self.txns.get(id).map_or(Err(Error::UnknownTransaction), f)
    }

    fn with_transaction_mut<F, R>(&mut self, id: &TID, f: F) -> Result<R>
        where F: FnOnce(&mut Transaction<RID>) -> Result<R> {
        self.txns.get_mut(id).map_or(Err(Error::UnknownTransaction), f)
    }
}

struct Transaction<RID> {
    created_commit: CID,
    state_type: TransactionState,
    read: HashSet<RID>,
    written: HashSet<RID>
}

impl <RID> Transaction<RID>
    where RID: Eq + Hash + Clone {
    fn new(created_commit: CID) -> Self {
        Transaction {
            created_commit,
            state_type: TransactionState::InProgress,
            read: HashSet::new(),
            written: HashSet::new()
        }
    }

    fn transaction_progress_many<R, W> (&mut self, read: R, written: W) -> Result<()>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        match self.state_type {
            TransactionState::InProgress => {
                self.read.extend(read.into_iter());
                self.written.extend(written.into_iter());
            },
            _ => return Err(Error::InvalidTransactionState)
        }
        Ok(())
    }

    fn transaction_progress_read(&mut self, read: RID) -> Result<()> {
        match self.state_type {
            TransactionState::InProgress => {
                self.read.insert(read);
            },
            _ => return Err(Error::InvalidTransactionState)
        }
        Ok(())
    }

    fn transaction_progress_write(&mut self, written: RID) -> Result<()> {
        match self.state_type {
            TransactionState::InProgress => {
                self.written.insert(written);
            },
            _ => return Err(Error::InvalidTransactionState)
        }
        Ok(())
    }

    fn wait_commit(&mut self) -> Result<()> {
        match self.state_type {
            TransactionState::InProgress => self.state_type = TransactionState::WaitCommit,
            _ => return Err(Error::InvalidTransactionState)
        }
        Ok(())
    }

    fn try_commit(&mut self, pending_writes: &HashSet<RID>) -> Result<bool> {
        match self.state_type {
            TransactionState::WaitCommit => {
                let can_commit = self.written.is_disjoint(pending_writes);
                if can_commit {
                    self.state_type = TransactionState::Committing;
                }
                Ok(can_commit)
            },
            _ => return Err(Error::InvalidTransactionState)
        }
    }

    fn commit_completed(&mut self) -> Result<()> {
        match self.state_type {
            TransactionState::Committing => {}
            _ => return Err(Error::InvalidTransactionState)
        }
        Ok(())
    }

    fn is_commit_prevented(&self, committed_writes: &HashSet<RID>) -> bool {
        match self.state_type {
            TransactionState::InProgress => !self.read.is_disjoint(committed_writes),
            TransactionState::WaitCommit => !self.read.is_disjoint(committed_writes),
            _ => false
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TransactionState {
    InProgress,
    WaitCommit,
    Committing
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    UnknownTransaction,
    InvalidTransactionState
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq)]
pub struct TransactionUpdate<TID> {
    can_commit: Vec<TID>,
    failed: Vec<TID>
}

impl <TID> TransactionUpdate<TID> {
    fn no_change() -> Self {
        TransactionUpdate {
            can_commit: Vec::new(),
            failed: Vec::new()
        }
    }

    fn with_can_commit(id: TID) -> Self {
        let mut result = Self::no_change();
        result.mark_can_commit(id);
        result
    }

    fn with_failed(id: TID) -> Self {
        let mut result = Self::no_change();
        result.mark_failed(id);
        result
    }

    fn mark_can_commit(&mut self, id: TID) {
        self.can_commit.push(id);
    }

    fn mark_failed(&mut self, id: TID) {
        self.failed.push(id);
    }

    fn merge(&mut self, other: Self) {
        self.can_commit.extend(other.can_commit);
        self.failed.extend(other.failed);
    }

    pub fn get_can_commit(&self) -> &Vec<TID> {
        &self.can_commit
    }

    pub fn get_failed(&self) -> &Vec<TID> {
        &self.failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_txn() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(&1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(&1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(&1));
    }

    #[test]
    fn blocking_txn() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        arbiter.start_transaction(2);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(&1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(&1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&2, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.start_commit(&2));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(2)), arbiter.commit_completed(&1));
    }

    #[test]
    fn non_blocking_txn() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        arbiter.start_transaction(2);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(&1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(&1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&2, 101));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(2)), arbiter.start_commit(&2));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(&2));
    }

    #[test]
    fn read_write_conflict_before_commit() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        arbiter.start_transaction(2);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(&1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(&2, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(&1));
        assert_eq!(Ok(TransactionUpdate::with_failed(2)), arbiter.commit_completed(&1));
    }

    #[test]
    fn read_write_conflict_after_commit() {
        let mut arbiter: Arbiter<i32, i32> = Arbiter::new();
        arbiter.start_transaction(1);
        arbiter.start_transaction(2);
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(&1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(&1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(1)), arbiter.start_commit(&1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(&1));
        assert_eq!(Ok(TransactionUpdate::with_failed(2)), arbiter.transaction_progress_read(&2, 100));
    }
}