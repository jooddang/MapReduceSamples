package matrix;

import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;
import matrix.MatrixAdder.Reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Reducer;

public class MatrixAdderTest extends TestCase {
	
	// We don't have to test Map because it extends IdentityMapper.
	
	public void testReduceSameSizes() throws IOException {
		Reducer<IntWritable, IntVectorWritable, IntWritable, IntVectorWritable> reducer = new Reduce();
		OutputCollectorMock<IntWritable, IntVectorWritable> output = new OutputCollectorMock<IntWritable, IntVectorWritable>();
		ArrayList<IntVectorWritable> list = new ArrayList<IntVectorWritable>();
		
		list.add(new IntVectorWritable(new int[]{1,2,3}));
		list.add(new IntVectorWritable(new int[]{6,5,4}));
		list.add(new IntVectorWritable(new int[]{9,9,9}));
		
		reducer.reduce(new IntWritable(1), list.iterator(), output, null);
		
		assertEquals(1, output.keys.size());
		assertEquals(1, output.keys.get(0).get());
		assertEquals(1, output.values.size());
		assertEquals(3, output.values.get(0).getSize());
		assertEquals(true, output.values.get(0).hasNext());
		assertEquals(16, output.values.get(0).next());
		assertEquals(16, output.values.get(0).next());
		assertEquals(16, output.values.get(0).next());
		assertEquals(false, output.values.get(0).hasNext());
	}
	
	public void testReduceDifferentSizes() throws IOException {
		Reducer<IntWritable, IntVectorWritable, IntWritable, IntVectorWritable> reducer = new Reduce();
		OutputCollectorMock<IntWritable, IntVectorWritable> output = new OutputCollectorMock<IntWritable, IntVectorWritable>();
		ArrayList<IntVectorWritable> list = new ArrayList<IntVectorWritable>();
		
		list.add(new IntVectorWritable(new int[]{1,2,3,4,5}));
		list.add(new IntVectorWritable(new int[]{6,5,4}));
		list.add(new IntVectorWritable(new int[]{9,9,9,1}));
		
		reducer.reduce(new IntWritable(1), list.iterator(), output, null);
		
		assertEquals(1, output.keys.size());
		assertEquals(1, output.keys.get(0).get());
		assertEquals(1, output.values.size());
		assertEquals(5, output.values.get(0).getSize());
		assertEquals(true, output.values.get(0).hasNext());
		assertEquals(16, output.values.get(0).next());
		assertEquals(16, output.values.get(0).next());
		assertEquals(16, output.values.get(0).next());
		assertEquals(true, output.values.get(0).hasNext());
		assertEquals(5, output.values.get(0).next());
		assertEquals(5, output.values.get(0).next());
		assertEquals(false, output.values.get(0).hasNext());
	}
}
