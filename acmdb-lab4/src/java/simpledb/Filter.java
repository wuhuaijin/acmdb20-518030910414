package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    private Predicate _p;
    private DbIterator _child;

    public Filter(Predicate p, DbIterator child) {
        _p = p;
        _child = child;
        // some code goes here
    }

    public Predicate getPredicate() {
        // some code goes here
        return _p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        _child.open();
        // some code goes here
    }

    public void close() {
        super.close();
        _child.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
        // some code goes here
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (true) {
           if (!_child.hasNext()) return null;
           else {
               Tuple next = _child.next();
               if (_p.filter(next)) return next;
           }
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        _child = children[0];
        // some code goes here
    }

}
