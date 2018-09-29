package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private HeapFile file;
    private TupleDesc td;
    private OpIterator[] opIterator;
    private boolean called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    		this.t=t;
    		this.child=child;
    		this.tableId=tableId;
    		this.file=(HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    		if (!child.getTupleDesc().equals(file.getTupleDesc())) {
    			throw new DbException("TupleDesc no match");
    		}
    		Type[] type=new Type[1];
    		type[0]=Type.INT_TYPE;
    		this.td=new TupleDesc(type);
    		this.called=false;
    }

    @Override
	public TupleDesc getTupleDesc() {
        // some code goes here
    		
        return this.td;
    }

    @Override
	public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    		super.open();
    		child.open();
    }

    @Override
	public void close() {
        // some code goes here
    		super.close();
    		child.close();
    }

    @Override
	public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		if (called) {
    			return null;
    		}
    		this.called=true;
    		int count=0;
    		while (child.hasNext()) {
	    		try {
	    			Tuple insertTuple=child.next();
	    			Database.getBufferPool().insertTuple(t, tableId, insertTuple);
				} catch (NoSuchElementException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new NoSuchElementException();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new DbException("");
				}
	        count++;
    		}
    		Tuple tuple=new Tuple(td);
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.opIterator;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    		this.opIterator=new OpIterator[1];
    		opIterator[0]=children[0];
    }
}
