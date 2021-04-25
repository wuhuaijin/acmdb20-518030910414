package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    private Catalog.Table _table;
    private int _tableid;
    private int _pgNo;


    public HeapPageId(int tableId, int pgNo) {
        _tableid = tableId;
        _table = Database.getCatalog().getTablefromId(tableId);
        _pgNo = pgNo;
        // some code goes here
    }

    /** @return the table associated with this PageId */
    public int getTableId() {

        // some code goes here
        return _tableid;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        return _pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return _tableid * 2333 + _pgNo;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if (!(o instanceof PageId)) return false;
        if (((PageId) o).getTableId() != this.getTableId() || ((PageId) o).pageNumber() != this.pageNumber()) return false;
        // some code goes here
        return true;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
