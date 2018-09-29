package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private OpIterator child;
    private int afield;
    private int gbfield;
    private Aggregator.Op aop;
    private Aggregator agg;
    private OpIterator[] children;
    private OpIterator aggIterator;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    		this.child=child;
    		this.aop=aop;
    		this.afield=afield;
    		this.gbfield=gfield;
    		TupleDesc td=child.getTupleDesc();
    		Type grType, aggType;
    		if (gfield==Aggregator.NO_GROUPING) {
    			grType=null;
    		} else {
    			grType=td.getFieldType(gfield);
    		}
    		aggType=td.getFieldType(afield);
    		if (aggType==Type.INT_TYPE) {
    			agg=new IntegerAggregator(gfield, grType, afield, aop);
    		} else if (aggType==Type.STRING_TYPE) {
    			agg=new StringAggregator(gfield, grType, afield, aop);
    		}
    		
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    		return this.gbfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    		if (this.gbfield==Aggregator.NO_GROUPING) {
    			return null;
    		}
    		return child.getTupleDesc().getFieldName(gbfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    		return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    		return this.child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    		return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    		return aop.toString();
    }

    @Override
	public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    		super.open();
    		this.child.open();
    		while (child.hasNext()) {
    			agg.mergeTupleIntoGroup(child.next());
    		}
    		aggIterator=agg.iterator();
    		aggIterator.open();
    		
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    		if (aggIterator.hasNext()) {
    			return aggIterator.next();
    		}
    		return null;
    }

    @Override
	public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    		child.rewind();
    		aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
	public TupleDesc getTupleDesc() {
	// some code goes here
    	
    		Type[] type;
    		String[] str;
    		if (gbfield==Aggregator.NO_GROUPING) {
    			type=new Type[1];
    			str=new String[1];
    			type[0]=Type.INT_TYPE;
    			str[0]=aop.toString() + "(" + child.getTupleDesc().getFieldName(afield)+')';
    		} else {
    			type=new Type[2];
    			type[0]=child.getTupleDesc().getFieldType(gbfield);
    			type[1]=Type.INT_TYPE;
    			str=new String[2];
    			str[0]=child.getTupleDesc().getFieldName(gbfield);
    			str[1]=aop.toString() + "(" + child.getTupleDesc().getFieldName(afield)+')';
    		}
    		
    		return new TupleDesc(type, str);
    		
    		//return child.getTupleDesc();
    }

    @Override
	public void close() {
	// some code goes here
    		super.close();
    		child.close();
    		aggIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
    		return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    		this.children=new OpIterator[1];
    		this.children[0]=children[0];
    }
    
}
