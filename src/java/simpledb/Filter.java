package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate p;
    private OpIterator child;
    private OpIterator[] childrenOp;
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
    		this.p=p;
    		this.child=child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    @Override
	public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    @Override
	public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    		super.open();
    		child.open();
    }

    @Override
	public void close() {
        // some code goes here
    		//super.close();
    		child.close();
    }

    @Override
	public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    @Override
	protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    		while (child.hasNext()) {
    			Tuple newTuple=child.next();
    			if (p.filter(newTuple)==true) {
    	    			return newTuple;
    	    		}
    		}
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    		return this.childrenOp;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    		childrenOp = new OpIterator[1];
    		childrenOp[0]=children[0];
    }

}
