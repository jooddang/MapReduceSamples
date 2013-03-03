package wtf;

import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import wtf.WordCount.Map;
import wtf.WordCount.Reduce;

public class WordCountTest extends TestCase {
	private OutputCollectorMock outputMock = new OutputCollectorMock();
	private Map mapper;

	public static class OutputCollectorMock implements OutputCollector<Text, IntWritable>{
		public ArrayList<Text> keys = new ArrayList<Text>();
		public ArrayList<IntWritable> values = new ArrayList<IntWritable>();

		public void collect (Text key, IntWritable value) throws IOException {
			keys.add(key);
			values.add(value);
		}
	}
	
	public void testOne() throws IOException {
		testMap("aaa");
		
		assertEquals(1, outputMock.keys.size());
		assertEquals("aaa", outputMock.keys.get(0).toString());
	}

	public void testTwo() throws IOException {
		testMap("a b");
		assertEquals(2, outputMock.keys.size());
		assertEquals("a", outputMock.keys.get(0).toString());
		assertEquals(1, outputMock.values.get(0).get());
	}
	
	public void testMap(String input) throws IOException {
		mapper = new Map();
		LongWritable k1 = new LongWritable(1);
		Text v1 = new Text(input);
		
		mapper.map(k1, v1, outputMock, null);
	}
	
	public void testReducer() throws IOException{
		Reduce reducer = new Reduce();
		Text k1 = new Text("aaa");
		
		ArrayList<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		reducer.reduce(k1, values.iterator(), outputMock, null);
		
		assertEquals(1, outputMock.keys.size());
	}
}
