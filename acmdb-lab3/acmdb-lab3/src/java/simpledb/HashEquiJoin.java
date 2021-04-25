package simpledb;

import jdk.nashorn.internal.scripts.JO;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */

    private JoinPredicate _p;
    private DbIterator _child1, _child2;
    private Tuple left, right;
    private Map<Field, ArrayList<Tuple>> map;

    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        _p = p;
        _child1 = child1;
        _child2 = child2;
        left = null;
        right = null;
        map = new LinkedHashMap<>();
        // some code goes here
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return _p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(_child1.getTupleDesc(), _child2.getTupleDesc());
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
        return _child1.getTupleDesc().getFieldName(_p.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return _child2.getTupleDesc().getFieldName(_p.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        _child2.open();
        _child1.open();
        map.clear();
        while (_child2.hasNext()){
            right = _child2.next();
            Field key = right.getField(_p.getField2());
            if (!map.containsKey(key)) map.put(key, new ArrayList<>());
            ArrayList<Tuple> Tuplelist = map.get(key);
            Tuplelist.add(right);
        }
        // some code goes here
    }

    public void close() {
        super.close();
        _child1.close();
        _child2.close();
        map.clear();
        listIt = null;
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
        // some code goes here
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (listIt != null && listIt.hasNext()){
            right = listIt.next();
            Tuple _return = new Tuple(this.getTupleDesc());
            for (int i = 0; i < _return.getTupleDesc().numFields(); ++i) {
                if (i < left.getTupleDesc().numFields()) _return.setField(i, left.getField(i));
                else _return.setField(i, right.getField(i-left.getTupleDesc().numFields()));
            }
            return _return;
        }

        while (_child1.hasNext()){
            left = _child1.next();
            Field key = left.getField(_p.getField1());

            ArrayList<Tuple> matchTupleList = map.get(key);
            if (matchTupleList == null) continue;
            listIt = matchTupleList.iterator();
            return fetchNext();
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{_child1, _child2};
        // some code goes here
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        _child1 = children[0];
        _child2 = children[1];
    }
    
}
