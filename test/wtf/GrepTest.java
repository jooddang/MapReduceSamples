package wtf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import wtf.Grep.Map;
import wtf.Grep.Reduce;


public class GrepTest extends TestCase {
	public static class OutputCollecterMock implements OutputCollector<NullWritable, Text> {
		
		public ArrayList<Text> values = new ArrayList<Text>();

		public void collect (NullWritable key, Text value) throws IOException {
			if (value != null) {
				values.add(value);
			}
		}
	}

	private OutputCollecterMock outputMock = new OutputCollecterMock();
	
	public void testOne() throws IOException{
		testMap(new Text("aaaa sdflskdfj iocjvp rldfk  aaaa sdfklsjdg"));
		assertEquals(1, outputMock.values.size());
	}
	
	public void testNothing() throws IOException {
		testMap(new Text("aakdk  dfkjsdog fgkflcv"));
		assertEquals(0, outputMock.values.size());
	}
	
	public void testMap(Text input) throws IOException {
		Map mapper = new Map();
		
		mapper.map(NullWritable.get(), input, outputMock, null);
	}
	
	public void testReduce1() throws IOException{
		testReduce();
	}
	
	public void testReduce() throws IOException {
		Reduce reducer = new Reduce();
		
		ArrayList<Text> value = new ArrayList<Text>();
		value.add(new Text("asdflkboclkd asdklgj cccld"));
		
		reducer.reduce(NullWritable.get(), value.iterator(), outputMock, null);
	}
}
