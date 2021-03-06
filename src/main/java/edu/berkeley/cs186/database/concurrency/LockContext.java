package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        LockContext curr_ancestor = parent;
        while (curr_ancestor != null) {
            if (!LockType.canBeParentLock(parent.lockman.getLockType(transaction, parent.name), lockType)) {
                throw new InvalidLockException("There is an ancestor lock that is preventing this acquire");
            }
            curr_ancestor = curr_ancestor.parent;
        }
        if (lockman.getLockType(transaction, name) != LockType.NL) {
            throw new DuplicateLockRequestException("Transaction already has a lock on name");
        }
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        }
        lockman.acquire(transaction, name, lockType);
        if (parent != null) {
            if (parent.numChildLocks.containsKey(transaction.getTransNum())) {
                Integer curr_value = parent.numChildLocks.get(transaction.getTransNum());
                parent.numChildLocks.replace(transaction.getTransNum(), curr_value + 1);
            } else {
                parent.numChildLocks.put(transaction.getTransNum(), 1);
            }
        }
        return;
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Context is read only");
        }
        if (lockman.getLockType(transaction, name) == LockType.NL) {
            throw new NoLockHeldException("No lock on this resource is held by transaction");
        }
        List<Lock> all_transaction_locks = lockman.getLocks(transaction);
        for (Lock l: all_transaction_locks) {
            LockContext curr_lock_context = fromResourceName(lockman, l.name);
            if (curr_lock_context.parent == this) {
                if (!LockType.canBeParentLock(LockType.NL, l.lockType)) {
                    throw new InvalidLockException("Lock cannot be released as it would violate multigranularity locking constraints");
                }
            }
        }
        lockman.release(transaction, name);
        if (parent != null) {
            Integer curr_value = parent.numChildLocks.get(transaction.getTransNum());
            parent.numChildLocks.replace(transaction.getTransNum(), curr_value - 1);
        }
        return;
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Context is read only");
        }
        LockType curr_type = lockman.getLockType(transaction, name);
        if (curr_type == newLockType) {
            throw new DuplicateLockRequestException("Transaction already has newlocktype lock");
        }
        if (curr_type == LockType.NL) {
            throw new NoLockHeldException("Transaction does not have a lock to promote");
        }

        if (newLockType == LockType.SIX) {
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("Invalid Promotion: Not substitutable and has ancestor of type SIX");
            }
            if (curr_type != LockType.S && curr_type != LockType.IS && curr_type != LockType.IX) {
                throw new InvalidLockException("Invalid Promotion: Not substitutable and current type is not IS, IX, or S");
            }
            List<ResourceName> sis_descendants = sisDescendants(transaction);
            int num_children_released = 0;
            for (ResourceName r: sis_descendants) {
                LockContext curr_context = fromResourceName(lockman, r);
                if (curr_context.parent == this) {
                    num_children_released += 1;
                }
            }
            sis_descendants.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, sis_descendants);
            int curr_num_children = numChildLocks.get(transaction.getTransNum());
            numChildLocks.replace(transaction.getTransNum(), curr_num_children - num_children_released);
            return;
        }
        if (!LockType.substitutable(newLockType, curr_type)) {
            throw new InvalidLockException("Invalid Promotion: Not substitutable and not being promoted to SIX");
        }

        if (parent != null && !LockType.canBeParentLock(parent.getEffectiveLockType(transaction), newLockType)) {
            throw new InvalidLockException("Invalid Promotion: Parent not compatible with new lock type");
        }

        /* Not sure if below code is necessary. It checks to see if the promotion will violate multigranularity
           by iterating through the children of current context. Reasoning that it's not necessary is that it would
           have failed the substitutable check.

        List<Lock> all_transaction_locks = lockman.getLocks(transaction);
        for (Lock l: all_transaction_locks) {
            LockContext curr_lock_context = fromResourceName(lockman, l.name);
            if (curr_lock_context.parent == this) {
                if (!LockType.canBeParentLock(newLockType, l.lockType)) {
                    throw new InvalidLockException("Lock cannot be released as it would violate multigranularity locking constraints");
                }
            }
        }
        */

        lockman.promote(transaction, name, newLockType);
        return;
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Context is read only");
        }
        LockType curr_type = lockman.getLockType(transaction, name);
        if (curr_type == LockType.NL) {
            throw new NoLockHeldException("Transaction does not have a lock at this level");
        }
        List<Lock> all_transaction_locks = lockman.getLocks(transaction);
        LockType least_permissive_type = LockType.S;
        if (curr_type == LockType.SIX || curr_type == LockType.IX | curr_type == LockType.X) {
            least_permissive_type = LockType.X;
        }
        List<ResourceName> to_release = new ArrayList<>();
        for (Lock l: all_transaction_locks) {
            if (l.name.isDescendantOf(name)) {
                if (l.lockType == LockType.SIX || l.lockType == LockType.IX || l.lockType == LockType.X) {
                    least_permissive_type = LockType.X;
                }
                if (l.lockType != LockType.NL) {
                    to_release.add(l.name);
                    LockContext curr_context = fromResourceName(lockman, l.name);
                    curr_context.numChildLocks.replace(transaction.getTransNum(), 0);
                }
            }
        }
        numChildLocks.replace(transaction.getTransNum(), 0);
        if (to_release.isEmpty() && curr_type == least_permissive_type) {
            return;
        }
        to_release.add(name);
        lockman.acquireAndRelease(transaction, name, least_permissive_type, to_release);
        return;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType explicit_type = lockman.getLockType(transaction, name);
        LockContext curr_ancestor = parent;
        while (curr_ancestor != null) {
            LockType curr_ancestor_type = curr_ancestor.lockman.getLockType(transaction, curr_ancestor.name);
            if (curr_ancestor_type == LockType.S || curr_ancestor_type == LockType.SIX) {
                return LockType.S;
            } else if (curr_ancestor_type == LockType.X) {
                return LockType.X;
            }
            curr_ancestor = curr_ancestor.parent;
        }
        return explicit_type;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext curr_ancestor = parent;
        while (curr_ancestor != null) {
            if (lockman.getLockType(transaction, name) == LockType.SIX) {
                return true;
            }
            curr_ancestor = curr_ancestor.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> all_transaction_locks = lockman.getLocks(transaction);
        List<ResourceName> descendant_sis_names = new ArrayList<>();
        for (Lock l: all_transaction_locks) {
            if (l.lockType == LockType.S || l.lockType == LockType.IS) {
                if (l.name.isDescendantOf(name)) {
                    descendant_sis_names.add(l.name);
                }
            }
        }
        return descendant_sis_names;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

