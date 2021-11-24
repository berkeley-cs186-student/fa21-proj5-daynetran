package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;
//import sun.rmi.transport.Endpoint;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry xactEntry = transactionTable.get(transNum);

        long newLSN = logManager.appendToLog(new CommitTransactionLogRecord(transNum, xactEntry.lastLSN)); // append a commit record to the log manager
        logManager.flushToLSN(newLSN); // flush the log

        xactEntry.transaction.setStatus(Transaction.Status.COMMITTING); // change xact status to Committing in Xact Table
        xactEntry.lastLSN = newLSN; // update xact lasLSN in Xact Table
        return newLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry xactEntry = transactionTable.get(transNum);

        long newLSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, xactEntry.lastLSN)); // append an abort record to the log manager

        xactEntry.transaction.setStatus(Transaction.Status.ABORTING); // change xact status to Aborting in Xact Table
        xactEntry.lastLSN = newLSN; // update xact lasLSN in Xact Table
        return newLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry xactEntry = transactionTable.get(transNum);

        if (xactEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0); // roll back changes if the transaction is aborting
        }

        transactionTable.remove(transNum); // transaction is removed from Xact Table
        xactEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        return logManager.appendToLog(new EndTransactionLogRecord(transNum, xactEntry.lastLSN)); // append an end record to the Xact Table
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        while (currentLSN > LSN) {
            LogRecord currentRecord = logManager.fetchLogRecord(currentLSN); // fetch current log record (of aborting xact)
            if (currentRecord.isUndoable()) { // can we undo this record's operation?
                LogRecord clr = currentRecord.undo(transactionEntry.lastLSN); // create a compensation log record
                long clrLSN = logManager.appendToLog(clr); // append the clr to the log
                transactionEntry.lastLSN = clrLSN; // update the lastLSN of the xact in the xact table
                clr.redo(this, this.diskSpaceManager, this.bufferManager); // call redo on the clr to perform the undo
            }
            currentLSN = currentRecord.getPrevLSN().orElse(LSN);
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);
        // Update the xact's lastLSN in xact table
        transactionEntry.lastLSN = LSN;
        // update dpt if page is new
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, LSN);
        }
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        int numDPTRecords = 0; int numTxnTableRecords = 0;
        for (long pageNum : dirtyPageTable.keySet()) {
            long recLSN = dirtyPageTable.get(pageNum);
            if (EndCheckpointLogRecord.fitsInOneRecord(numDPTRecords + 1, numTxnTableRecords)) {
                chkptDPT.put(pageNum, recLSN);
                numDPTRecords += 1;
            } else {
                LogRecord r = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(r);
                chkptDPT.clear();
                chkptTxnTable.clear();
                chkptDPT.put(pageNum, recLSN);
                numDPTRecords = 1;
                numTxnTableRecords = 0;
            }
        }
        for (long xactID : transactionTable.keySet()) {
            Transaction.Status status = transactionTable.get(xactID).transaction.getStatus();
            long lastLSN = transactionTable.get(xactID).lastLSN;
            if (EndCheckpointLogRecord.fitsInOneRecord(numDPTRecords, numTxnTableRecords + 1)) {
                chkptTxnTable.put(xactID, new Pair<>(status, lastLSN));
                numTxnTableRecords += 1;
            } else {
                LogRecord r = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(r);
                chkptDPT.clear();
                chkptTxnTable.clear();
                chkptTxnTable.put(xactID, new Pair<>(status, lastLSN));
                numDPTRecords = 0;
                numTxnTableRecords = 1;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> log = logManager.scanFrom(LSN);
        while (log.hasNext()) {
            LogRecord r = log.next();
            if (r.getTransNum().isPresent()) { // check whether the record produces a non-empty result for LogRecord#getTransNum()
                if (!transactionTable.containsKey(r.getTransNum().get())) { // check if xact isn't in Xact Table
                    Transaction t = newTransaction.apply(r.getTransNum().get()); // create new xact
                    startTransaction(t); // add xact to XactTtable
                }
                transactionTable.get(r.getTransNum().get()).lastLSN = r.getLSN(); // update xact's lastLSN
            }
            if ((r instanceof UpdatePageLogRecord) || (r instanceof UndoUpdatePageLogRecord)) { // check if record dirtied page w/out flushing to disk
                if (r.getPageNum().isPresent()) { // should be true bc we are dealing with UpdatePage and UndoUpdatePag records
                    long pageNum = r.getPageNum().get(); // retrieve pageNum from log record
                    if (!dirtyPageTable.containsKey(pageNum)) { // check that dpt does not contain pageNum of page dirtied by record
                        dirtyPageTable.put(pageNum, r.getLSN()); // add pageNum and recLSn to dpt
                    }
                }
            }
            if ((r instanceof FreePageLogRecord) || (r instanceof UndoAllocPageLogRecord)) { // check that record already flushed changes to disk
                long pageNum = r.getPageNum().get(); // retrieve pageNum from log record
                dirtyPageTable.remove(pageNum); // remove dirty page (and LSN) from dpt if it exists
            }
            if (r instanceof CommitTransactionLogRecord) { // check that log record r is commit type
                TransactionTableEntry xactEntry = transactionTable.get(r.getTransNum().get()); // retrieve xact entry corresponding to r from Xact Table
                xactEntry.transaction.setStatus(Transaction.Status.COMMITTING); // change xact status to Committing
                xactEntry.lastLSN = r.getLSN(); // update xact lastLSN in Xact Table
            }
            if (r instanceof AbortTransactionLogRecord) { // check that log record r is abort type
                TransactionTableEntry xactEntry = transactionTable.get(r.getTransNum().get()); // retrieve xact entry corresponding to r from Xact Table
                xactEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING); // change xact status to Recovery Aborting
                xactEntry.lastLSN = r.getLSN(); // update xact lastLSN in Xact Table
            }
            if (r instanceof EndTransactionLogRecord) { // check that log record r is end type
                TransactionTableEntry xactEntry = transactionTable.get(r.getTransNum().get()); // retrieve xact entry corresponding to r from Xact Table
                xactEntry.transaction.cleanup(); // clean up xact before setting status
                xactEntry.transaction.setStatus(Transaction.Status.COMPLETE); // set xact status to Complete
                transactionTable.remove(r.getTransNum().get()); // remove xact from Xact Table
                endedTransactions.add(r.getTransNum().get()); // added xact id to endedTransactions set, officially ending it
            }
            if (r instanceof EndCheckpointLogRecord) { // check that log record r is endchkpt type
                Map <Long, Long> chkptDPT = r.getDirtyPageTable(); // retrieve the chkpt's dirty page table
                for (long pageNum : chkptDPT.keySet()) // skim through each pageNum in the chkptDPT
                 {
                    long chkptRecLSN = chkptDPT.get(pageNum); // retrieve chkptRecLSN that corresponds to pageNum
                    if (!dirtyPageTable.containsKey(pageNum)) { // check that in-memory dpt does not contain pageNum
                        dirtyPageTable.put(pageNum, chkptRecLSN); // add pageNum and chkptRecLSN
                    } else { // observe that in-mem dpt already contains pageNum
                        dirtyPageTable.replace(pageNum, chkptRecLSN); // replace in-mem recLSN that corresponds with pageNum with more-accurate chkptRecLSN
                    }
                }
                Map <Long, Pair<Transaction.Status, Long>> chkptXactTable = r.getTransactionTable(); // retrieve the checkpoint's Xact Table
                for (long xactID : chkptXactTable.keySet()) // skim through each xactID in the chkptXactTable
                {
                    if (!endedTransactions.contains(xactID)) { // check that xactID didn't already end (as in be part of endedTransactions)
                        if (!transactionTable.containsKey(xactID)) { // check that in-mem Xact Table doesn't contain xactID
                            Transaction t = newTransaction.apply(xactID); // create new xact corresponding to xactID
                            startTransaction(t); // add xact t to in-memory Xact Table
                        }
                        if (transactionTable.get(xactID).lastLSN < chkptXactTable.get(xactID).getSecond()) // check that in-mem lastLSN < chkpt lastLSN
                        {
                            transactionTable.get(xactID).lastLSN = chkptXactTable.get(xactID).getSecond(); // update in-mem lastLSN to chkpt lastLSN
                        }
                        Transaction t = transactionTable.get(xactID).transaction; // retrieve xact corresponding to xactID in Xact Table
                        Transaction.Status inMemStatus = t.getStatus(); // retrieve in-mem status for xact
                        Transaction.Status chkptStatus = chkptXactTable.get(xactID).getFirst(); // retrieve chkpt status for xact
                        if ((inMemStatus == Transaction.Status.RUNNING) && (chkptStatus != Transaction.Status.RUNNING)) // check that in-mem xact is running and chkpt xact is not (i.e. greater)
                        {
                            if (chkptStatus == Transaction.Status.ABORTING) // check that chkptStatus is ABORTING
                            {
                                t.setStatus(Transaction.Status.RECOVERY_ABORTING); // call in-mem status RECOVERY_ABORTING
                            } else {
                                t.setStatus(chkptStatus); // call in-mem status COMMITTING or COMPLETE
                            }
                        } else if ((inMemStatus != Transaction.Status.COMPLETE) && (chkptStatus == Transaction.Status.COMPLETE)) { // implies inMemStatus is ABORTING OR COMMITTING, check that chkpSTATUS is COMPLETE (highest)
                            t.setStatus(chkptStatus); // set in-mem status to COMPLETE
                        }
                    }
                }
            }
        }
        for (long xactID : transactionTable.keySet()) { // skim through transactionTable's xactIDs
            TransactionTableEntry xactEntry = transactionTable.get(xactID); // retrieve xactEntry corresponding to xactID
            Transaction.Status status = xactEntry.transaction.getStatus(); // retrieve status corresponding to xactEntry
            if (status == Transaction.Status.COMMITTING) { // check that status is COMMITTING
                xactEntry.transaction.cleanup(); // call Transaction#cleanup() on xact before changing status
                xactEntry.transaction.setStatus(Transaction.Status.COMPLETE); // change xact status to COMPLETE
                transactionTable.remove(xactID); // remove xactID-xactEntry mapping from Xact Table
                logManager.appendToLog(new EndTransactionLogRecord(xactID, xactEntry.lastLSN)); // append EndTransactionLogRecord to logManager
            }
            if (status == Transaction.Status.RUNNING) { // check that status is RUNNING
                xactEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING); // change xact status to RECOVERY_ABORTING
                long lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(xactID, xactEntry.lastLSN)); // append AbortTransactionLogRecord to logManager
                xactEntry.lastLSN = lastLSN; // update lastLSN in Xact Table
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        if (dirtyPageTable.isEmpty()) {
            return;
        }
        long minRecLSN = Long.MAX_VALUE; // set initial minRecLSN to highest possible value
        for (long pageNum : dirtyPageTable.keySet()) // iterate pageNum over dpt keys
        {
            long recLSN = dirtyPageTable.get(pageNum); // retrieve the recLSN corresponding to pageNum
            if (recLSN < minRecLSN) { // check that recLSN is less than minRecLSN
                minRecLSN = recLSN; // set minRecLSN to recLSN
            }
        }
        Iterator<LogRecord> log = logManager.scanFrom(minRecLSN); // create iterator to scan logRecords from minRecLSN
        while (log.hasNext()) {
            LogRecord r = log.next();
            if (r.isRedoable()) { // check that log record r is redo-able
                if (r instanceof AllocPartLogRecord || r instanceof UndoAllocPartLogRecord || r instanceof FreePartLogRecord || r instanceof UndoFreePartLogRecord) { // check that r is a partition-related record
                    r.redo(this, this.diskSpaceManager, this.bufferManager); // redo the record
                }
                else if (r instanceof AllocPageLogRecord || r instanceof UndoFreePageLogRecord) { // check that r is a record that allocates a page (AllocPage, UndoFreePage)
                    r.redo(this, this.diskSpaceManager, this.bufferManager);
                }
                else if (r instanceof UpdatePageLogRecord || r instanceof UndoUpdatePageLogRecord || r instanceof FreePageLogRecord || r instanceof UndoAllocPageLogRecord) { // check that r is a record that modifies a page
                    if (r.getPageNum().isPresent()) { // check that the record has a pageNum
                        if (dirtyPageTable.containsKey(r.getPageNum().get())) { // check that the pageNum is in the dpt
                            if (r.getLSN() >= dirtyPageTable.get(r.getPageNum().get())) { // check that r's LSN is greater than or equal to the recLSN of the page
                                Page page = bufferManager.fetchPage(new DummyLockContext(), r.getPageNum().get()); // retrieve the page from the buffer manager
                                try {
                                    if (page.getPageLSN() < r.getLSN()) { // check that the page's lsn is strictly less than that of r
                                        r.redo(this, this.diskSpaceManager, this.bufferManager); // redo the record
                                    }
                                } finally {
                                    page.unpin(); // release the page from the memory buffers
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Long> abortLastLSN = new PriorityQueue<>(Collections.reverseOrder()); // initialize an empty PriorityQueue<Long> to queue largest LSNs
        for (Long xactID : transactionTable.keySet()) // iterate xact Ids over the Xact Table
        {
            TransactionTableEntry xactEntry = transactionTable.get(xactID); // retrieve the xact entry from Xact Table
            if (xactEntry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) { // check that the xact status is RECOVERY_ABORTING
                abortLastLSN.add(xactEntry.lastLSN); // add the xact entry LSN to the priority queue
            }
        }
        while (!abortLastLSN.isEmpty()) // continue until the priority queue is empty
        {
            long lastLSN = abortLastLSN.remove(); // remove the (highest) last LSN of an aborting xact from the queue
            LogRecord lastRecord = logManager.fetchLogRecord(lastLSN); // retrieve the log record of this xact's last LSN from log managers
            if (lastRecord.isUndoable()) { // check that lastRecord is undoable
                if (lastRecord.getTransNum().isPresent()) { // check that lastRecord does have a corresponding xactID
                    long xactID = lastRecord.getTransNum().get(); // retrieve xactID from lastRecord
                    TransactionTableEntry xactEntry = transactionTable.get(xactID); // retrieve xactEntry from Xact Table using xactID
                    LogRecord clr = lastRecord.undo(xactEntry.lastLSN); // create a CLR undoing lastRecord (implicitly, lastLSN of this xact is latsLSN)
                    xactEntry.lastLSN = logManager.appendToLog(clr); // append CLR to log manager and set lastLSN of xact to clrLSN using xactID
                    clr.redo(this, this.diskSpaceManager, this.bufferManager); // call redo on CLR
                }
            }
            long nextLSN = lastRecord.getUndoNextLSN().orElse(lastRecord.getPrevLSN().orElse((long) 0)); // retrieve nextLSN of Xact, either next undo-able or previous LSN or 0
            if (nextLSN != 0) { // check that we got the next undo-able LSN or the previous LSN
                abortLastLSN.add(nextLSN); // add to the priority queue
            } else { // implies that we got LSN 0
                if (lastRecord.getTransNum().isPresent()) { // check that lastRecord does have a corresponding xactID
                    long xactID = lastRecord.getTransNum().get(); // retrieve xactID from lastRecord
                    TransactionTableEntry xactEntry = transactionTable.get(xactID);
                    xactEntry.transaction.cleanup(); // clean up xact
                    xactEntry.transaction.setStatus(Transaction.Status.COMPLETE); // set xact status to COMPLETE
                    transactionTable.remove(xactID); // remove xact from Xact Table
                    logManager.appendToLog(new EndTransactionLogRecord(xactID, xactEntry.lastLSN)); // append an end record to the Xact Table
                }
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
