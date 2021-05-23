package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    private TransactionId _t;
    private DbIterator _child;
    private int _tableId;
    private int _cnt;

    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        _t = t;
        _child = child;
        _tableId = tableId;
        _cnt = -1;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        _child.open();
        _cnt = 0;
        while (_child.hasNext()){
            Tuple next = _child.next();
            try {
                Database.getBufferPool().insertTuple(_t, _tableId, next);
                _cnt++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // some code goes here
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (_cnt == -1) return null;
        Tuple insert_num = new Tuple(getTupleDesc());
        insert_num.setField(0, new IntField(_cnt));
        _cnt = -1;
        return insert_num;
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
