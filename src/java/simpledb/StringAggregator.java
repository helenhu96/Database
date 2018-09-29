package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field,Integer> map;
    private int overall;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		this.gbfield=gbfield;
		this.gbfieldtype=gbfieldtype;
		this.afield=afield;
		this.what=what;
		map=new HashMap<Field,Integer>();
		overall=0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
	public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    		Field key = tup.getField(this.gbfield);
    		int count=0;
    		if(map.containsKey(key))
    		{
    			count=map.get(key);
    		}
    		count++;
    		map.put(key, count);
    		overall++;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    @Override
	public OpIterator iterator() {
        // some code goes here
    		TupleDesc td;
        Type[] tp;
        ArrayList<Tuple> tuples = new ArrayList<>();
        
        if(gbfield==Aggregator.NO_GROUPING)
        {
        		tp=new Type[1];
        		tp[0]=Type.INT_TYPE;
        		td=new TupleDesc(tp);
        		Tuple tuple=new Tuple(td);
        		tuple.setField(0, new IntField(overall));
			tuples.add(tuple);
			return new TupleIterator(td,tuples);
        }
        	tp=new Type[2];
        	tp[0]=gbfieldtype;
        	tp[1]=Type.INT_TYPE;
        	td=new TupleDesc(tp);
        for(Field key: map.keySet())
        {
        		int val=0;
        		Tuple tuple=new Tuple(td);
        		if(what==Op.COUNT)
        		{
        			val=map.get(key);
        		}
        		else
        		{
        			throw new IllegalArgumentException();
        		}
        		tuple.setField(0, key);
    			tuple.setField(1, new IntField(val));
        		tuples.add(tuple);
        }
        return new TupleIterator(td,tuples);
    }
}