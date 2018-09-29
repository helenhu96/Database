package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private int ioCost;
    private DbFile file;
    private int tupleCount;
    private TupleDesc td;
    private int numfield;
    private DbFileIterator it;
    private HashMap<Integer, Integer> max;
    private HashMap<Integer, Integer> min;
    private HashMap<Integer, IntHistogram> intHistogram;
    private HashMap<Integer, StringHistogram> strHistogram;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     * @throws TransactionAbortedException 
     * @throws DbException 
     * @throws NoSuchElementException 
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    		this.ioCost = ioCostPerPage;
    		this.file = Database.getCatalog().getDatabaseFile(tableid);
    		this.td = file.getTupleDesc();
    		this.numfield = file.getTupleDesc().numFields();
    		this.it = file.iterator(new TransactionId());
    		this.max = new HashMap<>();
    		this.min = new HashMap<>();
    		createMaxMin(numfield, it);
    		this.intHistogram = createInt(numfield, it);
    		this.strHistogram = createStr(numfield, it);
    		try {
    			it.open();
				while (it.hasNext()) {
					Tuple t = it.next();
					this.tupleCount++;
					for (int i=0; i< td.numFields(); i++) {
						Field f = t.getField(i);
						if (f.getType() == Type.INT_TYPE) {
							int val = ((IntField) f).getValue();
							IntHistogram ihis = intHistogram.get(i);
							ihis.addValue(val);
						} else {
							String val = ((StringField) f).getValue();
							StringHistogram shis = strHistogram.get(i);
							shis.addValue(val);
						}
					}
				}
				it.close();
			} catch (NoSuchElementException | DbException | TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }

    public void createMaxMin(int numfield, DbFileIterator it) {
    		try {
    			it.open();
				while (it.hasNext()) {
					Tuple t = it.next();
					for (int i=0; i<numfield; i++) {
						Field f = t.getField(i);
						if (f.getType() == Type.INT_TYPE) {
							int val = ((IntField) f).getValue();
							if (!max.containsKey(i)) {
								max.put(i, val);
								min.put(i, val);
							}
							int maxVal = Math.max(val, max.get(i));
							int minVal = Math.min(val, min.get(i));
							max.put(i, maxVal);
							min.put(i, minVal);
						}
					}
				}
				it.close();
			} catch (NoSuchElementException | DbException | TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    public HashMap<Integer, IntHistogram> createInt(int numfield, DbFileIterator it) {
    		HashMap<Integer, IntHistogram> result = new HashMap<>();
    		for (int j = 0; j< numfield; j++) {
    			IntHistogram ihis = new IntHistogram(NUM_HIST_BINS, min.get(j), max.get(j));
    			result.put(j, ihis);
    		}
    		return result;
    }
    
    public HashMap<Integer, StringHistogram> createStr(int numfield, DbFileIterator it) {
    		HashMap<Integer, StringHistogram> result = new HashMap<>();
    		for (int i=0; i< numfield; i++) {
			StringHistogram strhis = new StringHistogram(NUM_HIST_BINS);
			result.put(i, strhis);
    		}
    		return result;
    }
    

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
    		HeapFile hp = (HeapFile) file;
    		return hp.numPages() * this.ioCost;
        
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(this.tupleCount * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    		Type type = td.getFieldType(field);
    		if (type.equals(Type.INT_TYPE)) {
    			int val = ((IntField) constant).getValue();
    			return intHistogram.get(field).estimateSelectivity(op, val);
    		} else {
    			String val = ((StringField) constant).getValue();
    			return strHistogram.get(field).estimateSelectivity(op, val);
    		}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tupleCount;
    }

}
