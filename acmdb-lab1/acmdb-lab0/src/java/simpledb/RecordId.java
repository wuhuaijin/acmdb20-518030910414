package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    private PageId _pid;
    private int _tupleno;
    public RecordId(PageId pid, int tupleno) {
        _pid = pid;
        _tupleno = tupleno;
        // some code goes here
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return _tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return _pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof RecordId)) return false;
        // some code goes here
        return ((RecordId) o)._pid.equals(this._pid) && ((RecordId) o)._tupleno == this._tupleno;

    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return _pid.hashCode() * 2333 + _tupleno;
        // some code goes here

    }

}
