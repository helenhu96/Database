package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private TupleDesc desc;
    private OpIterator[] childrenOp;
    private Tuple current1;
    private Tuple current2;
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
    		this.p=p;
    		this.child1=child1;
    		this.child2=child2;
    		TupleDesc td1=child1.getTupleDesc();
    		TupleDesc td2=child2.getTupleDesc();
    		this.desc=TupleDesc.merge(td1, td2);
    		this.current1=null;
    		this.current2=null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
    		return desc.getFieldName(p.getField1());
    }
    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
    		return desc.getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    @Override
	public TupleDesc getTupleDesc() {
        // some code goes here
    		return this.desc;
    }

    @Override
	public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
		super.open();	
    		child1.open();
    		child2.open();
    		this.current1=null;
    		this.current2=null;
    }

    @Override
	public void close() {
        // some code goes here
    		super.close();
    		child1.close();
    		child2.close();
    		this.current1=null;
    		this.current2=null;
    }

    @Override
	public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
		child1.rewind();
		child2.rewind();
		this.current1=null;
		this.current2=null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    @Override
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		
    		while (child1.hasNext() || current1!=null) {
    			if (current1==null) {
    				current1=child1.next();
    			}
    			Iterator<Field> i1=current1.fields();
    			while (child2.hasNext()) {
    				current2=child2.next();
    				Iterator<Field> i2=current2.fields();
    				if (p.filter(current1,current2)) {
    					Tuple result=new Tuple(this.getTupleDesc());
    					int i=0;
    					while (i1.hasNext()) {
    						Field i1_next=i1.next();
    						if (i1_next==null) {
    							break;
    						}
    						result.setField(i, i1_next);
    						i++;
    					}
    					while (i2.hasNext()) {
    						result.setField(i, i2.next());
    						i++;
    					}
    					return result; 
    				}
    			}
    			current1=null;
    			child2.rewind();
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
    		this.childrenOp = new OpIterator[2];
		childrenOp[0]=children[0];
		childrenOp[1]=children[1];
    }

}
