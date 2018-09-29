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

	private File file;
	private TupleDesc desc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    		this.file=f;
    		this.desc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
	public int getId() {
        // some code goes here
    		return file.getAbsolutePath().hashCode();
        
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
	public TupleDesc getTupleDesc() {
        // some code goes here
    		return desc;
    }

    // see DbFile.java for javadocs
    @Override
	public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
    		try {
				RandomAccessFile rf=new RandomAccessFile(this.file, "r");
				int offset=BufferPool.getPageSize()*(pid.getPageNumber());
				byte[] readByte=new byte[BufferPool.getPageSize()];
				rf.seek(offset);
				rf.readFully(readByte);
				rf.close();
				return new HeapPage((HeapPageId) pid,readByte);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException(e);
			}
    }

    // see DbFile.java for javadocs
    @Override
	public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    		RandomAccessFile rf=new RandomAccessFile(this.file, "rw");
    		int offset=BufferPool.getPageSize()*(page.getId().getPageNumber());
		byte[] writebyte=new byte[BufferPool.getPageSize()];
		rf.seek(offset);
		rf.write(writebyte, 0, BufferPool.getPageSize());
		rf.close();
			
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    		long fileSize=file.length();
    		int pageSize=BufferPool.getPageSize();
    		int numPages=(int) Math.ceil(fileSize*1.0/pageSize);
        return numPages;
    }

    // see DbFile.java for javadocs
    @Override
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    		ArrayList<Page> result = new ArrayList<Page>();
    		int id=this.getId();
    		int pgNum=0;
    		while (pgNum<this.numPages()) {
    			HeapPageId pid=new HeapPageId(id, pgNum);
    			HeapPage pg= (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    			if (pg.getNumEmptySlots()>0) {
    				pg.insertTuple(t);
    				result.add(pg);
    				return result;
    			} else {
    				pgNum++;
    			}
    		}
    		
    		HeapPageId pid=new HeapPageId(this.getId(), this.numPages());
    		HeapPage newPg=new HeapPage(pid, HeapPage.createEmptyPageData());
		newPg.insertTuple(t);
		RandomAccessFile rf=new RandomAccessFile(this.file, "rw");
		int offset=BufferPool.getPageSize()*this.numPages();
		rf.seek(offset);
		byte[] writeByte=newPg.getPageData();
		rf.write(writeByte, 0, BufferPool.getPageSize());
		rf.close();
		result.add(newPg);
		return result;

           
    		
    		
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    		ArrayList<Page> result=new ArrayList<>();
		RecordId recordid=t.getRecordId();
		PageId pid=recordid.getPageId();
		HeapPage pg=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		try {
			pg.deleteTuple(t);
		} catch (Exception e) {
			throw new DbException("Tuple Not On this Page");
		}
		result.add(pg);
		return result;
		
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
	public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    		return new HeapDbIterator(this, tid);
    }
    
    public class HeapDbIterator extends AbstractDbFileIterator {
    		Iterator<Tuple> pageTuple;
    		TransactionId tid;
    		HeapFile hpFile;
    		int currentPgNumber;
    		int numPages;
    		public HeapDbIterator(HeapFile hpFile, TransactionId tid) {
    			this.hpFile=hpFile;
    			this.tid=tid;
    			currentPgNumber=0;
    			numPages=hpFile.numPages();
    		}
    		
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			currentPgNumber=0;
			nextPage();
		}
		
		@Override
		public void close() {
			super.close();
			pageTuple=null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			currentPgNumber=0;
			nextPage();
		}

		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if (pageTuple == null) {
				return null;
			}
			if (pageTuple.hasNext()) {
				return pageTuple.next();
			} else {
				if (currentPgNumber<numPages()) {
					nextPage();
					if (pageTuple.hasNext()) {
						return pageTuple.next();
					}
				}
				pageTuple=null;
				return null;
			}
		}
		
		public void nextPage() throws TransactionAbortedException, DbException {
			HeapPageId hpId=new HeapPageId(hpFile.getId(), currentPgNumber);
			HeapPage hpPg=(HeapPage) Database.getBufferPool().getPage(tid, hpId, Permissions.READ_ONLY);
			pageTuple=hpPg.iterator();
			currentPgNumber++;
		}
    	
    }

}

