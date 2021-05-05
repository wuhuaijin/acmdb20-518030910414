package simpledb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private Map<Field, Integer> _counts;
    private TupleDesc _td;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
        _counts = new LinkedHashMap<>();
        if (gbfield == Aggregator.NO_GROUPING) {
            _td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        }
        else {
            _td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {"groupValue", "aggregateValue"});
        }
        // some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupfield;
        if (_gbfield == Aggregator.NO_GROUPING)
            groupfield = null;
        else
            groupfield = tup.getField(_gbfield);

        Integer cnt = _counts.getOrDefault(groupfield, 0);
        _counts.put(groupfield, cnt + 1);
        // some code goes here
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : _counts.entrySet()){
            Tuple tuple = new Tuple(_td);
            Integer value = entry.getValue();
            if (_gbfield == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(value));
            }
            else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(value));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(_td, tuples);
        // some code goes here
    }

}
