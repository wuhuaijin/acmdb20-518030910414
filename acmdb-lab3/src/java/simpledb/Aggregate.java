package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     *
     *
     */

    private DbIterator _child;
    private int _afield;
    private int _gfield;
    private Aggregator.Op _aop;
    private DbIterator _it;
    private Aggregator _aggregator;


    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        _child = child;
        _afield = afield;
        _gfield = gfield;
        _aop = aop;
        Type gbFieldType = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        if (child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE))
            _aggregator = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        else
            _aggregator = new StringAggregator(gfield, gbFieldType, afield, aop);
	    // some code goes here
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return _gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	    return _child.getTupleDesc().getFieldName(_gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	    return _child.getTupleDesc().getFieldName(_afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return _aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        _child.open();
        super.open();
        while(_child.hasNext()){
            _aggregator.mergeTupleIntoGroup(_child.next());
        }
        _it = _aggregator.iterator();
        _it.open();
	// some code goes here
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (_it.hasNext()){
            return _it.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
        _it.rewind();
	// some code goes here
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        TupleDesc child_td = _child.getTupleDesc();
        Type[] types;
        String[] names;
        String aggName = child_td.getFieldName(_afield);

        if (_gfield == Aggregator.NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{nameOfAggregatorOp(_aop) + "(" + aggName + ")"};
        }
        else {
            types = new Type[]{child_td.getFieldType(_gfield), Type.INT_TYPE};
            names = new String[]{child_td.getFieldName(_gfield), nameOfAggregatorOp(_aop) + "(" + aggName + ")"};
        }
        return new TupleDesc(types, names);
    }

    public void close() {
        super.close();
        _it.close();
        _child.close();
	// some code goes here
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
