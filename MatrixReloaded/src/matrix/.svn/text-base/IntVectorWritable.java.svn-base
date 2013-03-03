package matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class IntVectorWritable implements Writable {
	
	protected ArrayList<Integer> elements;
	private int currentPos;
	private int size;
	private int rowNum, colNum;
	
	public IntVectorWritable() {
		currentPos = 0;
		elements = new ArrayList<Integer>();
		size = 0;
		rowNum = -1;
		colNum = -1;
	}
	
	public IntVectorWritable(int[] values) {
		this();
		for (int value: values) {
			elements.add(value);
		}
		this.size = values.length;
	}
	
	public IntVectorWritable(int rowNum, int colNum) {
		this();
		this.rowNum = rowNum;
		this.colNum = colNum;
	}
	
	public IntVectorWritable(int rowNum, int colNum, int[] values) {
		this(values);
		this.rowNum = rowNum;
		this.colNum = colNum;
	}
	
	public IntVectorWritable(int initialSize) {
		this();
		this.size = initialSize;
	}
	
	/**
	 * Limits the number of elements in this vector manually.
	 * Useful not to clear when you reuse an instance repeatedly.
	 * 
	 * @param size
	 */
	public void setSize(int size) {
		this.size = size;
	}
	
	public int getSize() {
		return size;
	}
	
	public void setPosition(int rowNum, int colNum) {
		this.rowNum = rowNum;
		this.colNum = colNum;
	}
	
	public int getRow() {
		return rowNum;
	}
	
	public int getCol() {
		return colNum;
	}
	
	/**
	 * Checks if there is any left element after the current position.
	 * 
	 * @return True if more elements exist.
	 */
	public boolean hasNext() {
		return (currentPos < elements.size() && currentPos < this.size);
	}
	
	/**
	 * Clear all elements and set the size to 0.
	 * It is discouraged to clear everytime for implementation of map/reduce.
	 */
	@Deprecated
	public void clear() {
		elements.clear();
		size = 0;
	}
	
	/**
	 * Retrieves the element at the current position and moves the position pointer forward.
	 * 
	 * @return The value of the current element.
	 */
	public int next() {
		int value;
		if (!hasNext()) {
			throw new ArrayIndexOutOfBoundsException();	
		}
		value = elements.get(currentPos);
		currentPos += 1;
		return value;
	}
	
	/**
	 * Makes the position pointer to point at the first position.
	 */
	public void rewind() {
		currentPos = 0;
	}
	
	/**
	 * Retrieves the number of elements in the vector.
	 * Using getter/setter instead of this is more preferred.
	 * 
	 * @return Integer value indicating the size.
	 */
	@Deprecated
	public int size() {
		return size;
	}
	
	/**
	 * Set the element at the currnet position, or append if the current position is larger than the size of vector.
	 * 
	 * @param value An integer value to set.
	 */
	public void push(int value) {
		if (currentPos >= elements.size()) {
			elements.add(value);
			currentPos = elements.size();
		} else {
			elements.set(currentPos, value);
			currentPos += 1;
		}
	}
	
	public void set(int index, int value) {
		if (index >= size)
			throw new ArrayIndexOutOfBoundsException();
		elements.set(index, value);
	}
	
	public int get(int index) {
		if (index >= size)
			throw new ArrayIndexOutOfBoundsException();
		return elements.get(index);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int numCols = in.readInt();
		int i;
		setSize(numCols);
		rewind();
		for (i = 0; i < numCols; i++) {
			push(in.readInt());
		}
		rewind();
		size = numCols;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int i;
		out.writeInt(size);
		for (i = 0; i < size; i++) {
			out.writeInt(elements.get(i));
		}
		out.writeInt(size);
	}
	
	public int getCurrentPos() {
		return this.currentPos;
	}

}
