use linked_hash_map::LinkedHashMap;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;

pub type TID = u64;

pub struct Arbiter<RID> {
    next_id: TID,
    next_commit_id: TID,
    completed_txns: VecDeque<(TID, HashSet<RID>)>,
    pending_commits: HashSet<RID>,
    txns: LinkedHashMap<TID, TransactionState<RID>>
}

impl <RID> Arbiter<RID>
    where RID: Eq + Hash + Clone + Debug {
    pub fn new() -> Self {
        Arbiter {
            next_id: 0,
            next_commit_id: 0,
            completed_txns: VecDeque::new(),
            pending_commits: HashSet::new(),
            txns: LinkedHashMap::new()
        }
    }

    pub fn start_transaction(&mut self) -> TID {
        let id = self.next_id;
        self.next_id = self.next_id + 1;
        self.txns.insert(id.clone(), TransactionState::new(id, self.next_commit_id));
        id
    }

    pub fn transaction_progress_many<R, W>(&mut self, id: TID, read: HashSet<RID>, written: HashSet<RID>) -> Result<TransactionUpdate> {
        self.with_transaction(id, |txn| txn.transaction_progress_many(read, written))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_read(&mut self, id: TID, read: RID) -> Result<TransactionUpdate> {
        self.with_transaction(id, |txn| txn.transaction_progress_read(read))?;
        Ok(self.advance_txns())
    }

    pub fn transaction_progress_write(&mut self, id: TID, written: RID) -> Result<TransactionUpdate> {
        self.with_transaction(id, |txn| txn.transaction_progress_write(written))?;
        Ok(self.advance_txns())
    }

    pub fn start_commit(&mut self, id: TID) -> Result<TransactionUpdate> {
        self.with_transaction(id, |txn| txn.wait_commit())?;
        Ok(self.advance_txns())
    }

    pub fn commit_completed(&mut self, id: TID) -> Result<TransactionUpdate> {
        let txn = self.txns.remove(&id).ok_or_else(|| ArbiterError::UnknownTransaction(id))?;
        if txn.state_type != TransactionStateType::Committing {
            // TODO: This changes the order
            self.txns.insert(id, txn);
            return Err(ArbiterError::InvalidTransactionState(id));
        }

        let commit_id = self.next_commit_id;
        self.next_commit_id += 1;
        
        for rid in txn.written.iter() {
            self.pending_commits.remove(rid);
        }
        self.completed_txns.push_back((commit_id, txn.written));
        Ok(self.advance_txns())
    }

    fn advance_txns(&mut self) -> TransactionUpdate {
        let mut result = TransactionUpdate::no_change();
        result.merge(self.advance_impossible_writes());
        result.merge(self.advance_pending_commits());

        self.remove_obsolete_commits();
        
        result
    }

    fn advance_impossible_writes(&mut self) -> TransactionUpdate {
        let mut result = TransactionUpdate::no_change();
        for (_, txn) in self.txns.iter_mut().rev() {
            match txn.state_type {
                TransactionStateType::InProgress |
                TransactionStateType::WaitCommit => {
                    for (seq, committed_writes) in self.completed_txns.iter() {
                        if *seq >= txn.created_commit && txn.is_commit_prevented(&committed_writes) {
                            result.mark_failed(txn.id);
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

    fn advance_pending_commits(&mut self) -> TransactionUpdate {
        let mut result = TransactionUpdate::no_change();
        for (_, txn) in self.txns.iter_mut() {
            match txn.state_type {
                TransactionStateType::WaitCommit => {
                    if txn.try_commit(&self.pending_commits).unwrap() {
                        self.pending_commits.extend(txn.written.iter().cloned());
                        result.mark_can_commit(txn.id);
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

    fn with_transaction<F, R>(&mut self, id: TID, f: F) -> Result<R>
        where F: FnOnce(&mut TransactionState<RID>) -> Result<R> {
        self.txns.get_mut(&id).map_or(Err(ArbiterError::UnknownTransaction(id)), f)
    }
}

struct TransactionState<RID> {
    id: TID,
    created_commit: TID,
    state_type: TransactionStateType,
    read: HashSet<RID>,
    written: HashSet<RID>
}

impl <RID> TransactionState<RID>
    where RID: Eq + Hash + Clone {
    fn new(id: TID, created_commit: TID) -> TransactionState<RID> {
        TransactionState {
            id,
            created_commit,
            state_type: TransactionStateType::InProgress,
            read: HashSet::new(),
            written: HashSet::new()
        }
    }

    fn transaction_progress_many<R, W> (&mut self, read: R, written: W) -> Result<()>
        where R: IntoIterator<Item=RID>,
              W: IntoIterator<Item=RID> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.read.extend(read.into_iter());
                self.written.extend(written.into_iter());
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id))
        }
        Ok(())
    }

    fn transaction_progress_read(&mut self, read: RID) -> Result<()> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.read.insert(read);
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id))
        }
        Ok(())
    }

    fn transaction_progress_write(&mut self, written: RID) -> Result<()> {
        match self.state_type {
            TransactionStateType::InProgress => {
                self.written.insert(written);
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id))
        }
        Ok(())
    }

    fn wait_commit(&mut self) -> Result<()> {
        match self.state_type {
            TransactionStateType::InProgress => self.state_type = TransactionStateType::WaitCommit,
            _ => return Err(ArbiterError::InvalidTransactionState(self.id))
        }
        Ok(())
    }

    fn try_commit(&mut self, pending_writes: &HashSet<RID>) -> Result<bool> {
        match self.state_type {
            TransactionStateType::WaitCommit => {
                let can_commit = self.written.is_disjoint(pending_writes);
                if can_commit {
                    self.state_type = TransactionStateType::Committing;
                }
                Ok(can_commit)
            },
            _ => return Err(ArbiterError::InvalidTransactionState(self.id))
        }
    }

    fn commit_completed(&mut self) -> Result<()> {
        match self.state_type {
            TransactionStateType::Committing => {}
            _ => return Err(ArbiterError::InvalidTransactionState(self.id.clone()))
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
pub enum ArbiterError {
    UnknownTransaction(TID),
    InvalidTransactionState(TID)
}

pub type Result<T> = std::result::Result<T, ArbiterError>;

#[derive(Debug, PartialEq, Eq)]
pub struct TransactionUpdate {
    can_commit: Vec<TID>,
    failed: Vec<TID>
}

impl TransactionUpdate {
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
        let mut arbiter: Arbiter<i32> = Arbiter::new();
        let id = arbiter.start_transaction();
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(id, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id)), arbiter.start_commit(id));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(id));
    }

    #[test]
    fn blocking_txn() {
        let mut arbiter: Arbiter<i32> = Arbiter::new();
        let id1 = arbiter.start_transaction();
        let id2 = arbiter.start_transaction();
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(id1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id1)), arbiter.start_commit(id1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id2, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.start_commit(id2));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id2)), arbiter.commit_completed(id1));
    }

    #[test]
    fn non_blocking_txn() {
        let mut arbiter: Arbiter<i32> = Arbiter::new();
        let id1 = arbiter.start_transaction();
        let id2 = arbiter.start_transaction();
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(id1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id1)), arbiter.start_commit(id1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id2, 101));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id2)), arbiter.start_commit(id2));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(id2));
    }

    #[test]
    fn read_write_conflict_before_commit() {
        let mut arbiter: Arbiter<i32> = Arbiter::new();
        let id1 = arbiter.start_transaction();
        let id2 = arbiter.start_transaction();
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(id1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(id2, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id1)), arbiter.start_commit(id1));
        assert_eq!(Ok(TransactionUpdate::with_failed(id2)), arbiter.commit_completed(id1));
    }

    #[test]
    fn read_write_conflict_after_commit() {
        let mut arbiter: Arbiter<i32> = Arbiter::new();
        let id1 = arbiter.start_transaction();
        let id2 = arbiter.start_transaction();
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_read(id1, 100));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.transaction_progress_write(id1, 100));
        assert_eq!(Ok(TransactionUpdate::with_can_commit(id1)), arbiter.start_commit(id1));
        assert_eq!(Ok(TransactionUpdate::no_change()), arbiter.commit_completed(id1));
        assert_eq!(Ok(TransactionUpdate::with_failed(id2)), arbiter.transaction_progress_read(id2, 100));
    }
}