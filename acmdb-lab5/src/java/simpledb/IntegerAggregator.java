package simpledb;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private Map<Field, Integer> _groupByValue;
    private Map<Field, Integer> _counts;
    private TupleDesc _td;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
        _groupByValue = new LinkedHashMap<>();
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupfield;
        if (_gbfield == Aggregator.NO_GROUPING)
            groupfield = null;
        else
            groupfield = tup.getField(_gbfield);

        Integer oldvalue = _groupByValue.get(groupfield);
        Integer nowvalue = ((IntField) tup.getField(_afield)).getValue();
        Integer newvalue = null;

        switch (_what) {
            case MIN:
            {
                if (oldvalue == null) newvalue = nowvalue;
                else newvalue = Integer.min(oldvalue, nowvalue);
                break;
            }
            case MAX:
            {
                if (oldvalue == null) newvalue = nowvalue;
                else newvalue = Integer.max(oldvalue, nowvalue);
                break;
            }
            case COUNT:
            {
                if (oldvalue == null) newvalue = 1;
                else newvalue = oldvalue + 1;
                break;
            }
            case SUM:
            {
                if (oldvalue == null) newvalue = nowvalue;
                else newvalue = nowvalue + oldvalue;
                break;
            }
            case AVG:
            {
                if (oldvalue == null) newvalue = nowvalue;
                else newvalue = nowvalue + oldvalue;

                Integer cnt = _counts.getOrDefault(groupfield, 0);
                _counts.put(groupfield, cnt + 1);

                break;
            }
        }

        _groupByValue.put(groupfield, newvalue);
        // some code goes here
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList <Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : _groupByValue.entrySet()){
            Tuple tuple = new Tuple(_td);
            Integer value = entry.getValue();
            if (_what == Op.AVG) {
                value = value / _counts.get(entry.getKey());
            }

            if (_gbfield == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(value));
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(value));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(_td, tuples);
    }

}
