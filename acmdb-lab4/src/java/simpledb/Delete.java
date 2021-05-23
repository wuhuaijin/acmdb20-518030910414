package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId _t;
    private DbIterator _child;
    private int _cnt;

    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        _t = t;
        _child = child;
        _cnt = -1;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        _child.open();
        _cnt = 0;
        while (_child.hasNext()){
            Tuple next = _child.next();
            try {
                Database.getBufferPool().deleteTuple(_t, next);
                _cnt++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        super.close();
        _child.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
        // some code goes here
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (_cnt == -1) return null;
        Tuple delete_num = new Tuple(getTupleDesc());
        delete_num.setField(0, new IntField(_cnt));
        _cnt = -1;
        return delete_num;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        _child = children[0];
    }

}
