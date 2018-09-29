package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    
    public class AggHelper{
    		public Field key;
    		public int min;
    		public int max;
    		public int count;
    		public int sum;
    		public AggHelper(Field key){
    			this.key=key;
    			this.min=Integer.MAX_VALUE;
    			this.max=Integer.MIN_VALUE;
    			this.count=0;
    			this.sum=0;
    		}
    }
    
    private HashMap<Field,AggHelper> map;
    private AggHelper overall;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfieldS
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		this.gbfield=gbfield;
    		this.gbfieldtype=gbfieldtype;
    		this.afield=afield;
    		this.what=what;
    		map=new HashMap<Field, AggHelper>();
    		overall=new AggHelper(null);
    }
    
    

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
	public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    		Field key;
    		AggHelper agg;
    		if(gbfield==Aggregator.NO_GROUPING)
    		{
    			key=null;
    		}
    		else
    		{
    			key=tup.getField(gbfield);
    		}
    		if(map.containsKey(key))
    		{
    			agg=map.get(key);
    		}
    		else{
    			agg=new AggHelper(key);
    		}
    		int  value = ((IntField)tup.getField(afield)).getValue();
    		agg.count++;
    		overall.count++;
    		agg.sum=agg.sum+value;
    		overall.sum=overall.sum+value;
    		if(agg.min>value) {
    			agg.min=value;
    		}
    		if(overall.min>value) {
    			overall.min=value;
    		}
    		if(agg.max<value){
    			agg.max=value;
    		}
    		if(overall.max<value){
    			overall.max=value;
    		}
    		map.put(key, agg);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
        		int val=0;
        		switch(this.what)
        		{
        			case MIN: 
        				val=overall.min;
        				break;
        			case MAX:
        				val=overall.max;
        				break;
        			case SUM:
        				val=overall.sum;
        				break;
        			case AVG:
        				val=overall.sum/overall.count;
        				break;
        			case COUNT:
        				val=overall.count;
        				break;
        		}
        		tuple.setField(0, new IntField(val));
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
        		switch(this.what)
        		{
        			case MIN: 
        				val=map.get(key).min;
        				break;
        			case MAX:
        				val=map.get(key).max;
        				break;
        			case SUM:
        				val=map.get(key).sum;
        				break;
        			case AVG:
        				val=map.get(key).sum/map.get(key).count;
        				break;
        			case COUNT:
        				val=map.get(key).count;
        				break;
        		}
        		tuple.setField(0, key);
    			tuple.setField(1, new IntField(val));
        		tuples.add(tuple);
        }
        return new TupleIterator(td,tuples);
    }
}
