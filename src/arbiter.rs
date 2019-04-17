use linked_hash_map::LinkedHashMap;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;

type CID = u64;

pub struct Arbiter<TID, RID> {
    next_commit_id: CID,
    completed_txns: VecDeque<(CID, HashSet<RID>)>,
    pending_commits: HashSet<RID>,
    txns: LinkedHashMap<TID, TransactionState<RID>>
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

    pub fn start_transaction(&mut self, id: TID) {
        self.txns.insert(id, TransactionState::new(self.next_commit_id));
    }

    pub fn transaction_progress_many<R, W>(&mut self, id: &TID, read: R, written: W) -> Result<TransactionUpdate<TID>, TID>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        self.with_transaction(id, |txn| txn.transaction_progress_many(id, read, written))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_read(&mut self, id: &TID, read: RID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id, |txn| txn.transaction_progress_read(id, read))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_write(&mut self, id: &TID, written: RID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id, |txn| txn.transaction_progress_write(id, written))?;
        Ok(self.advance_txns())
    }

    pub fn start_commit(&mut self, id: &TID) -> Result<TransactionUpdate<TID>, TID> {
        self.with_transaction(id, |txn| txn.wait_commit(id))?;
        Ok(self.advance_txns())
    }

    pub fn commit_completed(&mut self, id: &TID) -> Result<TransactionUpdate<TID>, TID> {
        let txn = self.txns.remove(&id).ok_or_else(|| Error::UnknownTransaction(id.clone()))?;
        if txn.state_type != TransactionStateType::Committing {
            // TODO: This changes the order
            self.txns.insert(id.clone(), txn);
            return Err(Error::InvalidTransactionState(id.clone()));
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
                TransactionStateType::InProgress |
                TransactionStateType::WaitCommit => {
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
                TransactionStateType::WaitCommit => {
                    if txn.try_commit(id, &self.pending_commits).unwrap() {
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

    fn with_transaction<F, R>(&mut self, id: &TID, f: F) -> Result<R, TID>
        where F: FnOnce(&mut TransactionState<RID>) -> Result<R, TID> {
        self.txns.get_mut(id).map_or(Err(Error::UnknownTransaction(id.clone())), f)
    }
}

struct TransactionState<RID> {
    created_commit: CID,
    state_type: TransactionStateType,
    read: HashSet<RID>,
    written: HashSet<RID>
}

impl <RID> TransactionState<RID>
    where RID: Eq + Hash + Clone {
    fn new(created_commit: CID) -> TransactionState<RID> {
        TransactionState {
            created_commit,
            state_type: TransactionStateType::InProgress,
            read: HashSet::new(),
            written: HashSet::new()
        }
    }

    fn transaction_progress_many<TID: Clone, R, W> (&mut self, id: &TID, read: R, written: W) -> Result<(), TID>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.read.extend(read.into_iter());
                self.written.extend(written.into_iter());
            },
            _ => return Err(Error::InvalidTransactionState(id.clone()))
        }
        Ok(())
    }

    fn transaction_progress_read<TID: Clone>(&mut self, id: &TID, read: RID) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.read.insert(read);
            },
            _ => return Err(Error::InvalidTransactionState(id.clone()))
        }
        Ok(())
    }

    fn transaction_progress_write<TID: Clone>(&mut self, id: &TID, written: RID) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.written.insert(written);
            },
            _ => return Err(Error::InvalidTransactionState(id.clone()))
        }
        Ok(())
    }

    fn wait_commit<TID: Clone>(&mut self, id: &TID) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::InProgress => self.state_type = TransactionStateType::WaitCommit,
            _ => return Err(Error::InvalidTransactionState(id.clone()))
        }
        Ok(())
    }

    fn try_commit<TID: Clone>(&mut self, id: &TID, pending_writes: &HashSet<RID>) -> Result<bool, TID> {
        match self.state_type {
            TransactionStateType::WaitCommit => {
                let can_commit = self.written.is_disjoint(pending_writes);
                if can_commit {
                    self.state_type = TransactionStateType::Committing;
                }
                Ok(can_commit)
            },
            _ => return Err(Error::InvalidTransactionState(id.clone()))
        }
    }

    fn commit_completed<TID: Clone>(&mut self, id: &TID) -> Result<(), TID> {
        match self.state_type {
            TransactionStateType::Committing => {}
            _ => return Err(Error::InvalidTransactionState(id.clone()))
        }
        Ok(())
    }

    fn is_commit_prevented(&self, committed_writes: &HashSet<RID>) -> bool {
        match self.state_type {
            TransactionStateType::InProgress => !self.read.is_disjoint(committed_writes),
            TransactionStateType::WaitCommit => !self.read.is_disjoint(committed_writes),
            _ => false
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TransactionStateType {
    InProgress,
    WaitCommit,
    Committing
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error<TID> {
    UnknownTransaction(TID),
    InvalidTransactionState(TID)
}

pub type Result<T, TID> = std::result::Result<T, Error<TID>>;

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