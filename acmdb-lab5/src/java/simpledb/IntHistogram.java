package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int _bucketsNum;
    private int _min;
    private int _max;
    private int _width;
    private int[] _buckets;
    private int _tuplesNum;

    public IntHistogram(int buckets, int min, int max) {
        if (max - min + 1 < buckets) _bucketsNum = max - min + 1;
        else _bucketsNum = buckets;
        _min = min;
        _max = max;
        _width = (max - min + 1) / _bucketsNum;
        _buckets = new int[_bucketsNum];
        _tuplesNum = 0;
    	// some code goes here
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */

    private int getBucket(int value) {
        if (value - _min + 1 <= _width * (_bucketsNum - 1))
            return (value - _min) / _width;
        else return _bucketsNum - 1;
    }

    public void addValue(int v) {
        int i = getBucket(v);
        _tuplesNum++;
        _buckets[i]++;
    	// some code goes here
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */


    private int getBucketWidth(int id) {
        if (id == _bucketsNum - 1) {
            return (_max - _min + 1) - _width * (_bucketsNum - 1);
        }
        else return _width;
    }

    public double estimateSelectivity(Predicate.Op op, int v) {

        int i = getBucket(v);
        double tot = 0.0;
        switch (op) {
            case EQUALS:
                if (v < _min || v > _max) return 0.0;
                tot = _buckets[i] * 1.0 / getBucketWidth(i);
                break;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case LESS_THAN:
                if (v <= _min) return 0.0;
                if (v > _max) return 1.0;
                for (int ii = 0; ii < i; ++ii) {
                    tot += _buckets[ii];
                }
                int tmp = i * _width + 1;
                tot += _buckets[i] * (v - tmp) * 1.0 / getBucketWidth(i);
                break;
            case GREATER_THAN:
                if (v < _min) return 1.0;
                if (v >= _max) return 0.0;
                for (int ii = i + 1; ii < _bucketsNum; ii++){
                    tot += _buckets[ii];
                }
                int tmp2 = i * _width + getBucketWidth(i);
                tot += _buckets[i] * (tmp2 - v) * 1.0 / getBucketWidth(i);
                break;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
    	// some code goes here
        return tot / _tuplesNum;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
