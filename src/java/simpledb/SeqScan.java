package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private TupleDesc desc;
    private HeapFile hpFile;
    private DbFileIterator iterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    		this.reset(tableid, tableAlias);
    		this.tid=tid;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    		this.tableid=tableid;
    		this.tableAlias=tableAlias;
    		this.desc=Database.getCatalog().getTupleDesc(tableid);
    		Type[] descType=new Type[desc.numFields()];
    		String[] descName=new String[desc.numFields()];
    		for (int i=0; i<desc.numFields(); i++) {
    			descType[i]=desc.fields.get(i).fieldType;
    			descName[i]=tableAlias + "." + desc.fields.get(i).fieldName;
    			
    		}
    		this.desc=new TupleDesc(descType, descName);
    		
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    @Override
	public void open() throws DbException, TransactionAbortedException {
        // some code goes here
     	hpFile=(HeapFile) Database.getCatalog().getDatabaseFile(tableid);
     	iterator=hpFile.iterator(tid);
     	iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    @Override
	public TupleDesc getTupleDesc() {
        // some code goes here
        return this.desc;
    }

    
    
    @Override
	public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		return iterator.hasNext();

    }

    @Override
	public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    		//return Database.getCatalog().getDatabaseFile(tableid).iterator(tid).next();
    		return iterator.next();
    		
    }

    @Override
	public void close() {
        // some code goes here
    		iterator.close();
    }

    @Override
	public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    		iterator.rewind();
    }
}
