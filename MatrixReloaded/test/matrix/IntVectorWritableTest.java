package matrix;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class IntVectorWritableTest extends TestCase {
	public void testConstructor() {
		IntVectorWritable vector = new IntVectorWritable();
		assertEquals(0, vector.elements.size());
	}
	
	public void testIteration() {
		int i = 1;
		IntVectorWritable vector = new IntVectorWritable(new int[]{1,2,3});
		assertEquals(3, vector.getSize());
		while (vector.hasNext()) {
			assertEquals(i, vector.next());
			i += 1;
		}
		assertEquals(4, i);
		vector.rewind();
	}
		
	public void testPushSequentialWithSizeLimit() {
		IntVectorWritable vector = new IntVectorWritable(new int[]{5,6,7,8,9});
		assertEquals(5, vector.getSize());
		vector.rewind();
		assertEquals(true, vector.hasNext());
		assertEquals(5, vector.next());
		assertEquals(6, vector.next());
		assertEquals(7, vector.next());
		assertEquals(true, vector.hasNext());
		vector.setSize(0);
		assertEquals(false, vector.hasNext());
		vector.setSize(3);
		vector.rewind();
		assertEquals(true, vector.hasNext());
		vector.push(1);
		vector.push(2);
		vector.push(3);
		assertEquals(false, vector.hasNext());
		vector.rewind();
		assertEquals(1, vector.next());
		assertEquals(2, vector.next());
		assertEquals(3, vector.next());
		assertEquals(false, vector.hasNext());
		vector.setSize(5);
		assertEquals(true, vector.hasNext());
		assertEquals(8, vector.next());
		assertEquals(9, vector.next());
		assertEquals(false, vector.hasNext());
		vector.setSize(6);
		assertEquals(false, vector.hasNext());
	}
	
	public void testReadWrite() throws IOException {
		IntVectorWritable vector = new IntVectorWritable(new int[]{123, 456, 789});
		DataOutputBuffer out = new DataOutputBuffer();
		DataInputBuffer in = new DataInputBuffer();
		
		// Write to the output buffer first.
		vector.write(out);
		
		// Moves data from the output buffer to the input buffer.
		in.reset(out.getData(), out.getLength());
		
		// Read from the input buffer.
		vector.readFields(in);
		
		assertEquals(123, vector.next());
		assertEquals(456, vector.next());
		assertEquals(789, vector.next());
		assertEquals(false, vector.hasNext());
	}
}
