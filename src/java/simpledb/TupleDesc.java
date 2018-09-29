package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	public ArrayList<TDItem> fields;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
       

        @Override
		public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        
        return this.fields.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        
    		fields=new ArrayList<>();
		for (int i=0; i<typeAr.length; i++) {
			if (fieldAr[i]==null) {
				this.fields.add(new TDItem(typeAr[i], ""));
			} else {
				this.fields.add(new TDItem(typeAr[i], fieldAr[i]));
			}
		}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        
    		fields=new ArrayList<>();
    		for (int i=0; i<typeAr.length; i++) {
    			this.fields.add(new TDItem(typeAr[i],""));
    		}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        
        return this.fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        
    		if (i<0 || i>=fields.size()) {
    			throw new NoSuchElementException();
    		}
    		return fields.get(i).fieldName;
    
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        
    		if (i<0 || i>=this.fields.size()) {
    			throw new NoSuchElementException();
    		} 
    		return fields.get(i).fieldType;
    	}

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
        for (int i=0; i<fields.size(); i++) {
        		if (this.fields.get(i).fieldName.equals(name)) {
        			return i;
        		}
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        
    		int sum = 0;
        for (int i=0; i<this.numFields(); i++) {
        		sum += this.getFieldType(i).getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        
    		//TupleDesc result=new TupleDesc();
    		Type[] types=new Type[td1.numFields()+td2.numFields()];
    		String[] names=new String[td1.numFields()+td2.numFields()];
    		int i,j;
    		for (i=0; i<td1.numFields(); i++) {
    			types[i]=td1.getFieldType(i);
    			names[i]=td1.getFieldName(i);
    		}
    		for (j=0; j<td2.numFields(); j++) {
    			types[i+j]=td2.getFieldType(j);
    			names[i+j]=td2.getFieldName(j);
    		}
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
	public boolean equals(Object o) {
        
    		if (o==null) return false;
    		if (!(o instanceof TupleDesc)) {
    			return false;
    		}
    		if (this.numFields()!=((TupleDesc) o).numFields()) {
    			return false;
    		}
    		for (int i=0; i<this.numFields(); i++) {
    			if (!this.getFieldType(i).equals(((TupleDesc) o).getFieldType(i))) {
    				return false;
    			}
    		}
    		return true;
    }

    @Override
	public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
	public String toString() {
        
    		String res="";
    		for (int i=0; i<this.numFields(); i++) {
    			res += this.getFieldType(i)+"(";
    			res += this.getFieldName(i)+"),";
    		}
        return res;
    }
}
