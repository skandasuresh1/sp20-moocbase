package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock l: locks) {
                if (l.transactionNum != except && !LockType.compatible(lockType, l.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            long lock_transaction = lock.transactionNum;
            for (Lock l: locks) {
                if (l.transactionNum == lock_transaction) {
                    l.lockType = lock.lockType;
                    return;
                }
            }
            if (transactionLocks.containsKey(lock_transaction)) {
                transactionLocks.get(lock_transaction).add(lock);
            } else {
                List<Lock> new_list = new ArrayList<>();
                new_list.add(lock);
                transactionLocks.put(lock_transaction, new_list);
            }
            locks.add(lock);
            return;
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            long lock_transaction = lock.transactionNum;
            transactionLocks.get(lock_transaction).remove(lock);
            locks.remove(lock);
            processQueue();
            return;
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         * Must be called from within a synchronized block.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            while (!waitingQueue.isEmpty()) {
                LockRequest curr_request = waitingQueue.peek();
                if (checkCompatible(curr_request.lock.lockType, curr_request.transaction.getTransNum())) {
                    waitingQueue.removeFirst();
                    grantOrUpdateLock(curr_request.lock);
                    for (Lock l: curr_request.releasedLocks) {
                        releaseLock(l);
                    }
                    curr_request.transaction.unblock();
                } else {
                    break;
                }
            }
            return;
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock l: locks) {
                if (l.transactionNum == transaction) {
                    return l.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(proj4_part1): You may add helper methods here if you wish
    private List<Lock> getTransactionLocks(TransactionContext t, List<ResourceName> releaseLocks) {
        ArrayList<Lock> ret = new ArrayList<>();
        for (ResourceName name: releaseLocks) {
            ResourceEntry curr_entry = getResourceEntry(name);
            for (Lock l: curr_entry.locks) {
                if (l.transactionNum == t.getTransNum()) {
                    ret.add(l);
                }
            }
        }
        return ret;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            boolean has_lock = false;
            for (Lock l: entry.locks) {
                if (l.transactionNum == transaction.getTransNum() && l.lockType != LockType.NL && !releaseLocks.contains(name)) {
                    throw new DuplicateLockRequestException("transaction already has a lock on resource and is not being released");
                }
            }
            for (ResourceName r_name: releaseLocks) {
                ResourceEntry curr_entry = getResourceEntry(r_name);
                if (curr_entry.getTransactionLockType(transaction.getTransNum()) == LockType.NL) {
                    throw new NoLockHeldException("transaction doesn't hold a lock on a resource in releaseLocks");
                }
            }
            shouldBlock = !entry.checkCompatible(lockType, transaction.getTransNum());
            Lock new_lock = new Lock(name, lockType, transaction.getTransNum());
            if (!shouldBlock) {
                entry.grantOrUpdateLock(new_lock);
                for (ResourceName r_name: releaseLocks) {
                    List<Lock> locks_to_release = new ArrayList<>();
                    ResourceEntry curr_entry = getResourceEntry(r_name);
                    for (Lock l: curr_entry.locks) {
                        if (l.transactionNum == transaction.getTransNum() && r_name != name) {
                            locks_to_release.add(l);
                        }
                    }
                    for (Lock l: locks_to_release) {
                        curr_entry.releaseLock(l);
                    }
                }
            } else {
                List<Lock> releasedLocks = getTransactionLocks(transaction, releaseLocks);
                LockRequest new_request = new LockRequest(transaction, new_lock, releasedLocks);
                entry.addToQueue(new_request, true);
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
        return;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            for (Lock l: entry.locks) {
                if (l.transactionNum == transaction.getTransNum() && l.lockType != LockType.NL) {
                    throw new DuplicateLockRequestException("transaction already has a lock on resource");
                }
            }
            if (!entry.checkCompatible(lockType, transaction.getTransNum()) || !entry.waitingQueue.isEmpty()) {
                shouldBlock = true;
            } else {
                shouldBlock = false;
            }

            Lock new_lock = new Lock(name, lockType, transaction.getTransNum());
            if (!shouldBlock) {
                entry.grantOrUpdateLock(new_lock);
            } else {
                LockRequest new_request = new LockRequest(transaction, new_lock);
                entry.addToQueue(new_request, false);
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
        return;
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock lock_to_release;
            for (Lock l: entry.locks) {
                if (l.transactionNum == transaction.getTransNum() && l.lockType == LockType.NL) {
                    throw new NoLockHeldException("Transaction holds an NL lock on this resource");
                } else if (l.transactionNum == transaction.getTransNum()){
                    lock_to_release = l;
                    entry.releaseLock(lock_to_release);
                    return;
                }
            }
            throw new NoLockHeldException("Transaction doesn't hold a lock on this resource");
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            LockType curr_type = entry.getTransactionLockType(transaction.getTransNum());
            if (curr_type == newLockType) {
                throw new DuplicateLockRequestException("Transaction already has a newlocktype lock on resource");
            } else if (curr_type == LockType.NL) {
                throw new NoLockHeldException("Transaction has no lock on resource");
            } else if (!LockType.substitutable(newLockType, curr_type)) {
                throw new InvalidLockException("New lock type cannot be substitued for current lock type");
            }
            for (Lock l: entry.locks) {
                if (l.transactionNum != transaction.getTransNum() && !LockType.compatible(l.lockType, newLockType)) {
                    shouldBlock = true;
                }
            }
            Lock upgraded = new Lock(name, newLockType, transaction.getTransNum());
            if (!shouldBlock) {
                entry.grantOrUpdateLock(upgraded);
            } else {
                LockRequest new_request = new LockRequest(transaction, upgraded);
                entry.addToQueue(new_request, true);
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
        return;
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        LockType ret = LockType.NL;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            for (Lock l: entry.locks) {
                if (l.transactionNum == transaction.getTransNum()) {
                    ret =  l.lockType;
                }
            }
        }
        return ret;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
