package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */

    public static void promoteOrAcquire(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (lockContext.getEffectiveLockType(transaction) == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else {
            try {
                lockContext.promote(transaction, lockType);
            } catch (InvalidLockException e) {
                lockContext.escalate(transaction);
            }

        }
    }

    public static void checkAncestors(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (lockContext.parent == null) {
            promoteOrAcquire(lockContext, lockType);
            return;
        }
        if (LockType.canBeParentLock(lockContext.parent.getEffectiveLockType(transaction), lockType)) {
            promoteOrAcquire(lockContext, lockType);
            return;
        } else {
            switch (lockType) {
                case S:
                    checkAncestors(lockContext.parent, LockType.IS);
                    break;
                case X:
                    checkAncestors(lockContext.parent, LockType.IX);
                    break;
                case IS:
                    checkAncestors(lockContext.parent, LockType.IS);
                    break;
                case IX:
                    checkAncestors(lockContext.parent, LockType.IX);
                    break;
                case SIX:
                    checkAncestors(lockContext.parent, LockType.IX);
                    break;
            }
            promoteOrAcquire(lockContext, lockType);
            return;
        }
    }
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null) {
            return;
        }
        LockType curr_type = lockContext.getEffectiveLockType(transaction);

        if (lockType == LockType.NL) {
            return;
        }
        if (LockType.substitutable(curr_type, lockType)) {
            return;
        }
        LockType new_type = lockType;
        switch (lockType) {
            case S:
                if (curr_type == LockType.NL || curr_type == LockType.IS) {
                    new_type = LockType.S;
                } else if (curr_type == LockType.IX) {
                    new_type = LockType.SIX;
                }
                break;
            case X:
                new_type = LockType.X;
                break;
        }
        checkAncestors(lockContext, new_type);
        if (new_type == LockType.S || new_type == LockType.X) {
            lockContext.escalate(transaction);
        }
        return;
    }

    // TODO(proj4_part2): add helper methods as you see fit
}
