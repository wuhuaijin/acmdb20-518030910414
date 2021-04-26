package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
       
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> _tdArray = new ArrayList<TDItem>();
    private int _byte_size = 0;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return _tdArray.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int _size = 0;
        for (int i = 0; i < typeAr.length; ++i) {
            if (fieldAr[i] != null)
                _tdArray.add(new TDItem(typeAr[i], fieldAr[i]));
            else _tdArray.add(new TDItem(typeAr[i], null));
            _size += typeAr[i].getLen();
        }
        _byte_size = _size;
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int _size = 0;
        for (int i = 0; i < typeAr.length; ++i) {
            _tdArray.add(new TDItem(typeAr[i], null));
            _size += typeAr[i].getLen();
        }
        _byte_size = _size;
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {

        // some code goes here
        return _tdArray.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= _tdArray.size()) throw new NoSuchElementException();
        return _tdArray.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= _tdArray.size()) throw new NoSuchElementException();
        return _tdArray.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) throw new NoSuchElementException();
        for (int i = 0; i < _tdArray.size(); ++i) {
            if (getFieldName(i) == null) continue;
            if (getFieldName(i).equals(name)) return i;
        }
        throw new NoSuchElementException();
        // some code goes here
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return this._byte_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int _num1 = td1.numFields();
        int _num2 = td2.numFields();
        Type[] _merge_type = new Type[_num1 + _num2];
        String[] _merge_string = new String[_num1 + _num2];
        for (int i = 0; i < _num1; ++i) {
            _merge_type[i] = td1.getFieldType(i);
            _merge_string[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < _num2; ++i) {
            _merge_type[i + _num1] = td2.getFieldType(i);
            _merge_string[i + _num1] = td2.getFieldName(i);
        }
        // some code goes here
        return new TupleDesc(_merge_type, _merge_string);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {

        if (!(o instanceof TupleDesc)) return false;
        if (this.numFields() != ((TupleDesc) o).numFields()) return false;
        for (int i = 0; i < this.numFields(); ++i) {
            if (!(this.getFieldType(i).equals(((TupleDesc) o).getFieldType(i)))) return false;
        }
        // some code goes here
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int _hash_code = 1;
        for (int i = 0; i < _tdArray.size(); ++i) {
            _hash_code = _hash_code * 7 + getFieldType(i).hashCode();
        }
        return _hash_code;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = "";
        for (int i = 0; i < _tdArray.size() - 1; ++i) {
            result = result + _tdArray.get(i).toString() + ",";
        }

        // some code goes here
        return result;
    }
}
