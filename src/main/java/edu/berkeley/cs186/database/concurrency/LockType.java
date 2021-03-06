package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (a) {
            case S:
                if (b == IS || b == S || b == NL) {
                    return true;
                } else {
                    return false;
                }
            case X:
                if (b == NL) {
                    return true;
                } else {
                    return false;
                }
            case IS:
                if (b == X) {
                    return false;
                } else {
                    return true;
                }
            case IX:
                if (b == IS || b == IX || b == NL) {
                    return true;
                } else {
                    return false;
                }
            case SIX:
                if (b == IS || b == NL) {
                    return true;
                } else {
                    return false;
                }
            case NL:
                return true;
        }
        throw new NullPointerException("reached end of switch and never returned");
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (childLockType == NL) {
            return true;
        }
        switch (parentLockType) {
            case S:
                return false;
            case X:
                return false;
            case IS:
                if (childLockType == S || childLockType == IS) {
                    return true;
                } else {
                    return false;
                }
            case IX:
                return true;
            case SIX:
                if (childLockType == X || childLockType == IX) {
                    return true;
                } else {
                    return false;
                }
            case NL:
                return false;
        }
        throw new NullPointerException("reached end of switch statement and didn't return");
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (substitute == required) {
            return true;
        }
        if (required == NL) {
            return true;
        }
        if (substitute == NL) {
            return false;
        }
        switch (substitute) {
            case S:
                return false;
            case X:
                if (required == S) {
                    return true;
                } else {
                    return false;
                }
            case IS:
                return false;
            case IX:
                if (required == IS) {
                    return true;
                } else {
                    return false;
                }
            case SIX:
                if (required == S) {
                    return true;
                } else {
                    return false;
                }
        }
        throw new NullPointerException("made it to end of switch and didn't return");
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

