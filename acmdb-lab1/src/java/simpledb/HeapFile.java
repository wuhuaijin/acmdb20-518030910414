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
    private class HeapFileIterator implements DbFileIterator{
        private TransactionId tid;
        private int page_pos;
        private Iterator<Tuple> tuple_it;
        public HeapFileIterator(TransactionId tid)
        {
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            page_pos = 0;
            HeapPageId pid = new HeapPageId(getId(), page_pos);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            tuple_it = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuple_it == null) return false;
            if (tuple_it.hasNext()) return true;
            if (page_pos < numPages() - 1)
            {
                HeapPageId pid = new HeapPageId(getId(), ++page_pos);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                tuple_it = page.iterator();
                if (tuple_it.hasNext())
                    return true;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext())
                throw new NoSuchElementException();
            return tuple_it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            page_pos = 0;
            tuple_it = null;
        }

    }
    private File file;
    private TupleDesc td;
    private int pageNumber;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        pageNumber = (int) (file.length()) / (BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try{
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pid.pageNumber() * BufferPool.getPageSize());
            byte[] data = HeapPage.createEmptyPageData();
            raf.read(data, 0, data.length);
            return new HeapPage(new HeapPageId(pid.getTableId(), pageNumber), data);
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
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
        return pageNumber;
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

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

