package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */


class PageLock {
    public PageId _pageId;
    private Set<TransactionId> _shared_locks;
    private TransactionId _ex_lock;

    PageLock(PageId pid) {
        _pageId = pid;
        _shared_locks = Collections.synchronizedSet(new HashSet<>());
        _ex_lock = null;
    }

    boolean acquireLock(TransactionId tid, Permissions p) {
        if (p.equals(Permissions.READ_ONLY)) {
            if (_ex_lock == null) {
                _shared_locks.add(tid);
                return true;
            } else {
                return _ex_lock.equals(tid);
            }
        } else if (p.equals(Permissions.READ_WRITE)) {
            if (_ex_lock != null) return _ex_lock.equals(tid);
            if (_shared_locks.isEmpty() || (_shared_locks.size() == 1 && _shared_locks.contains(tid))) {
                _ex_lock = tid;
                _shared_locks.clear();

                return true;
            }
            return false;
        }
        return false;
    }

    void releaseLock(TransactionId tid) {
        if (tid.equals(_ex_lock)) _ex_lock = null;
        else _shared_locks.remove(tid);
    }


    boolean holdsLock(TransactionId tid) {
        return _ex_lock.equals(tid) || _shared_locks.contains(tid);
    }

    boolean hasEx() {
        return _ex_lock != null;
    }

    Set<TransactionId> getHolders() {
        Set<TransactionId> set = new HashSet<>();
        if (hasEx()) set.add(_ex_lock);
        set.addAll(_shared_locks);
        return set;
    }

}


public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */


    private ConcurrentHashMap<PageId, PageLock> pidToLock;
    private ConcurrentHashMap<TransactionId, Set<PageId>> tidToPid;
    private DependencyGraph dependencyGraph;

    private class DependencyGraph {
        private ConcurrentHashMap<TransactionId, Set<TransactionId>> graph = new ConcurrentHashMap<>();
        synchronized void updateGraph(TransactionId tid, PageId pid) {
            graph.putIfAbsent(tid, new HashSet<>());
            Set<TransactionId> set = graph.get(tid);
            set.clear();
            if (pid == null) return;
            synchronized (pidToLock.get(pid)) {
                set.addAll(pidToLock.get(pid).getHolders());
            }
        }

        synchronized void checkDeadlock(TransactionId tid) throws TransactionAbortedException {
            dfs(tid, new HashSet<>());
        }


        void dfs(TransactionId root, HashSet<TransactionId> visit) throws TransactionAbortedException {
            if (!graph.containsKey(root)) return;
            for (TransactionId i : graph.get(root)) {
                if (visit.contains(i)) {
                    throw new TransactionAbortedException();
                }
                visit.add(i);
                dfs(i, visit);
                visit.remove(i);
            }
        }

    }


//    private final LockManager lockManager;

    private LinkedHashMap<PageId, Page> _page_cache;
    private LinkedHashMap<PageId, Integer> _LRU_cache;
//    private ConcurrentHashMap<PageId, Page> _page_cache;
//    private ConcurrentHashMap<PageId, Integer> _LRU_cache;
    private int _capacity;

    public BufferPool(int numPages) {
        _page_cache = new LinkedHashMap<>();
        _LRU_cache = new LinkedHashMap<>();
        _capacity = numPages;
        tidToPid = new ConcurrentHashMap<>();
        pidToLock = new ConcurrentHashMap<>();
        dependencyGraph = new DependencyGraph();
//        lockManager = new LockManager();
        // some code goes here
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */




    public Page findPage(PageId pid) {
        for (int i = 0; i < _page_cache.size(); ++i) {

            if (_page_cache.get(i).getId().equals(pid)) {

                return _page_cache.get(i);

            }
        }
        return null;
    }

    public void LRU_update(PageId pid) {
        for (PageId key : _LRU_cache.keySet()) {
            int time = _LRU_cache.get(key);
            if (time > 0) _LRU_cache.put(key, time - 1);
        }
        _LRU_cache.put(pid, _capacity);

    }

    private void acquireLock(PageId pid, TransactionId tid, Permissions p) throws TransactionAbortedException {
        pidToLock.putIfAbsent(pid, new PageLock(pid));
        boolean ifGetLock;
        synchronized (pidToLock.get(pid)) {
            ifGetLock = pidToLock.get(pid).acquireLock(tid, p);
        }
        while (!ifGetLock) {
            dependencyGraph.updateGraph(tid, pid);
            dependencyGraph.checkDeadlock(tid);
            synchronized (pidToLock.get(pid)) {
                ifGetLock = pidToLock.get(pid).acquireLock(tid, p);
            }
        }
        dependencyGraph.updateGraph(tid, null);
        tidToPid.putIfAbsent(tid, new HashSet<>());
        tidToPid.get(tid).add(pid);

    }

    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

//        if (pid == null) throw

        acquireLock(pid, tid, perm);
//        lockManager.acquireLock(tid, pid, perm);

        if (_page_cache.containsKey(pid)) return _page_cache.get(pid);
        if (_page_cache.size() >= _capacity) {
            evictPage();
//            throw new DbException("full");
        }
        Page _page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        _page_cache.put(pid, _page);
        _page.setBeforeImage();
//        LRU_update(pid);

        return _page;

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        synchronized (pidToLock.get(pid)){
            pidToLock.get(pid).releaseLock(tid);
        }
        tidToPid.get(tid).remove(pid);
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (pidToLock.get(p)) {
            return pidToLock.get(p).holdsLock(tid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        Set<PageId> LockPages = tidToPid.get(tid);
        tidToPid.remove(tid);
        if (LockPages == null) return;
        for (PageId pid: LockPages){
            Page page = _page_cache.get(pid);
            if (page != null && pidToLock.get(pid).hasEx()){
                if (commit) {
                    if (page.isDirty() != null) {
                        flushPage(pid);
                        page.setBeforeImage();
                    }
                }
                else {
                    _page_cache.put(pid, page.getBeforeImage());
                }
            }
            synchronized (pidToLock.get(pid)){
                pidToLock.get(pid).releaseLock(tid);
            }
        }
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile tablefile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = tablefile.insertTuple(tid, t);
        for (Page i : pages) {
            PageId pid = i.getId();
            if (!_page_cache.containsKey(pid) && _page_cache.size() >= _capacity) evictPage();
            i.markDirty(true, tid);
            _page_cache.put(pid, i);
//            LRU_update(pid);
        }

        // some code goes here
        // not necessary for lab1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affected_page = tableFile.deleteTuple(tid, t);
        for (Page i: affected_page){
            PageId pid = i.getId();
            if (!_page_cache.containsKey(pid) && _page_cache.size() >= _capacity) evictPage();
            i.markDirty(true, tid);
            _page_cache.put(pid, i);
//            LRU_update(pid);
        }
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId i : _page_cache.keySet()) {
            flushPage(i);
        }
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
//        if (!_page_cache.containsValue(pid)) return;
//        try {
//            flushPage(pid);
//        } catch (IOException e){
//            e.printStackTrace();
//        }
        _page_cache.remove(pid);
//        LRU_update(pid);
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = _page_cache.get(pid);
        if (page == null) throw new IOException();
        if (page.isDirty() == null) return;
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        page.markDirty(false, null);

        // some code goes here
        // not necessary for lab1
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
//        int times = _capacity;
//        PageId evict_pageId = null;
//        for (PageId pageId : _LRU_cache.keySet()) {
//            if (_LRU_cache.get(pageId) <= times) {
//                times = _LRU_cache.get(pageId);
//                evict_pageId = pageId;
//            }
//        }
//        try {
//            flushPage(evict_pageId);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        _page_cache.remove(evict_pageId);
//        _LRU_cache.remove(evict_pageId);

        for (Map.Entry<PageId, Page> i : _page_cache.entrySet()) {
            PageId pid = i.getKey();
            Page page = i.getValue();
            if (page.isDirty() == null) {
                discardPage(pid);
                return;
            }
        }
        throw new DbException("shit");
        // some code goes here
        // not necessary for lab1
    }

}
