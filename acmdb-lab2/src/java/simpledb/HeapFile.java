package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private TupleDesc _tupleDesc;
    private File _file;
    private int _num_page;

    public HeapFile(File f, TupleDesc td) {
        _file = f;
        _num_page = (int) (_file.length() / BufferPool.getPageSize());
        _tupleDesc = td;
        // some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return _file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return getFile().getAbsoluteFile().hashCode();
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return _tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
//        return null;
        Page _page = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try{
            RandomAccessFile _rf = new RandomAccessFile(_file, "r");
            _rf.seek(pid.pageNumber()*BufferPool.getPageSize());
            _rf.read(data, 0, BufferPool.getPageSize());
            _page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        return _page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return _num_page;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator {

        private TransactionId _tid;
        private int _currentPid;
        private Iterator<Tuple> _tupleIterator;
    
        public HeapFileIterator(TransactionId tid) {
            _tid = tid;
        }
    
        @Override
        public void open() throws DbException, TransactionAbortedException {
            _currentPid = 0;
            PageId pageId = new HeapPageId(getId(), _currentPid);
            _tupleIterator = ((HeapPage) Database.getBufferPool().getPage(_tid, pageId, Permissions.READ_ONLY)).iterator();
        }
    
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (_tupleIterator == null) return false;
            if (_tupleIterator.hasNext()) return true;
            return _currentPid < numPages() - 1;
        }
    
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException();
            if (_tupleIterator.hasNext()) return _tupleIterator.next();
            _currentPid += 1;
            PageId pageId = new HeapPageId(getId(), _currentPid);
            _tupleIterator = ((HeapPage) Database.getBufferPool().getPage(_tid, pageId, Permissions.READ_ONLY)).iterator();
            return _tupleIterator.next();
    
        }
    
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }
    
        @Override
        public void close() {
            _currentPid = 0;
            _tupleIterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        // some code goes here
        return new HeapFileIterator(tid);
    }

}

