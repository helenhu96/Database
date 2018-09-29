package simpledb;

import java.util.*;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	private int min;
	private int max;
	private int buckets;
	private int w_b;
	private int ntups;
	HashMap <Integer, Integer> map;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    		this.buckets = buckets;
    		this.min = min;
    		this.max = max;
    		this.w_b = (int) Math.ceil((max - min + 1) * 1.0 / buckets);
    		map = new HashMap<>();
    		for (int i=0; i<buckets; i++) {
    			map.put(i, 0);
    		}
    		ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    
    public void addValue(int v) {
    	// some code goes here
    		ntups ++;
    		int index = (v - min)/w_b;
    		map.put(index, map.get(index)+1);	
    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
    		double left = 0;
    		double right = 0;
    		double equal = 0;
    		if (v < min) {
    			right = 1;
    		} else if (v > max) {
    			left = 1;
    		} else {
	    		int b = (int) ((v - this.min)/w_b);
	    		int h_b = map.get(b);
	    		double b_f = h_b / ntups;
	    		double b_right = (b + 1) * w_b + min;
	    		double b_left = b * w_b + min;
	    		double r_b_part = (b_right - v) / w_b;
	    		double l_b_part = (v - b_left) / w_b;
	    		int r_count = 0;
	    		int l_count = 0;
	    		for (int i = 0; i < b; i++) {
	    			l_count += map.get(i);
	    		}
	    		for (int i = b + 1; i < buckets; i++) {
	    			r_count += map.get(i);
	    		}
	    		equal = helpEqual(v) / ntups;
	    		left = l_b_part * b_f + l_count*1.0 / ntups;
	    		right = r_b_part * b_f + r_count*1.0 / ntups;
    		}
    		switch(op) {
    		case EQUALS:
    			return equal;
    		
    		case GREATER_THAN:
    			return right;
    		
    		case LESS_THAN:
    			return left;
    			
    		case LESS_THAN_OR_EQ:
    			return left + equal;
    		case GREATER_THAN_OR_EQ:
    			return right + equal;
    		case LIKE:
    			return equal;
    		case NOT_EQUALS:
    			return 1-equal;
    		}
    		return 0;
    	}
      
 
    public double helpEqual(int v) {
		int index = (int) ((v - this.min)/w_b);
		int height = map.get(index);
		return height * 1.0 / w_b;
    }
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
	public String toString() {
        // some code goes here
        return null;
    }
}
