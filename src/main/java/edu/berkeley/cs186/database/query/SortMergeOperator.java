package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            // Sort both tables
            SortOperator left_sort = new SortOperator(getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
            SortOperator right_sort = new SortOperator(getTransaction(), this.getRightTableName(), new RightRecordComparator());
            String sorted_left = left_sort.sort();
            String sorted_right = right_sort.sort();

            this.leftIterator = SortMergeOperator.this.getRecordIterator(sorted_left);
            this.rightIterator = SortMergeOperator.this.getRecordIterator(sorted_right);
            this.nextRecord = null;
            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }


        }

        private void resetRightRecord() {
            this.rightIterator.reset();
            assert(rightIterator.hasNext());
            rightRecord = rightIterator.next();
        }

        private void fetchNextRecord() {
            this.nextRecord = null;
            while (this.nextRecord == null) {
                while (leftIterator.hasNext() && rightIterator.hasNext()) {
                    if (!marked) {
                        leftRecord = leftIterator.next();
                        rightRecord = rightIterator.next();
                        DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                        DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                        if (leftJoinValue.compareTo(rightJoinValue) == 0) {
                            this.rightIterator.markPrev();
                            this.marked = true;
                            break;
                        } else if (leftJoinValue.compareTo(rightJoinValue) > 0) {
                            this.rightRecord = this.rightIterator.next();
                        } else {
                            this.leftRecord = this.leftIterator.next();
                        }
                    } else {
                        break;
                    }
                }
                if (!leftIterator.hasNext() || !rightIterator.hasNext()) {
                    throw new NoSuchElementException("No new record to fetch");
                }
                DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                if (leftJoinValue.equals(rightJoinValue)) {
                    List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues);
                    this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                } else {
                    this.leftRecord = leftIterator.hasNext() ? leftIterator.next(): null;
                    resetRightRecord();
                    marked = false;
                }
            }
        }

        /*
           Advances left and right iterators until we arrive at a match
         */
        public boolean findMatch() {
            while(this.leftIterator.hasNext() && this.rightIterator.hasNext()) {
                DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                if (leftJoinValue.compareTo(rightJoinValue) == 0) {
                    this.rightIterator.markPrev();
                    this.marked = true;
                    return true;
                } else if (leftJoinValue.compareTo(rightJoinValue) > 0) {
                    this.rightRecord = this.rightIterator.next();
                } else {
                    this.leftRecord = this.leftIterator.next();
                }
            }
            return false;
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement

            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement

            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
