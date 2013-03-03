package matrixadd;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class MatrixAdd {
	public static class Map extends MapReduceBase 
	implements Mapper<LongWritable, Text, NullWritable, Text> {

		String regexp;

		public void configure (JobConf job) {
			super.configure(job);
			regexp = job.get("regexp");
		}

		@Override
		public void map (LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
		throws IOException {
			// TODO Auto-generated method stub
			if (value.toString().indexOf(regexp) != -1) {
				output.collect(NullWritable.get(), value);
			}
		}

	}
	
	public static class Reduce extends IdentityReducer<NullWritable, Text> implements Reducer<NullWritable, Text, NullWritable, Text> {
		// not implement
	}
	
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "hdfs://192.168.2.204:9300");
		conf.set("mapred.job.tracker", "192.168.2.204:9301");
		
		// TODO
		conf.set("hadoop.job.ugi", "openable,supergroup");
		conf.set("mapred.jar", "F:/Programing/java/HadoopStudy090602/wordcount.jar");
		
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// TODO
		FileInputFormat.setInputPaths(conf, "/user/openable/wordcount_output");
		FileOutputFormat.setOutputPath(conf, new Path("/user/openable/grep_output"));
		
		JobClient client = new JobClient(conf);
		client.submitJob(conf);
	}
	
}


class IntVector {
	public void pushBack (int e) {
		
	}
}


class MatrixRecordReader implements RecordReader<IntWritable, IntVector> {
	private LineRecordReader lineInput;
	
	public MatrixRecordReader (Configuration job, FileSplit split) throws IOException {
		lineInput = new LineRecordReader(job, (FileSplit) split);
	}
	
	public MatrixRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
		lineInput = new LineRecordReader(in, offset, endOffset, maxLineLength);
	}
	
	public MatrixRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
		lineInput = new LineRecordReader(in, offset, endOffset, job);
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lineInput.close();
	}

	@Override
	public IntWritable createKey() {
		// TODO Auto-generated method stub
		return new IntWritable();
	}

	@Override
	public IntVector createValue() {
		// TODO Auto-generated method stub
		return new IntVector();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return lineInput.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return lineInput.getProgress();
	}

	@Override
	public boolean next(IntWritable key, IntVector value) throws IOException {
		// TODO Auto-generated method stub
		Text line_value = new Text();
		boolean bool = lineInput.next(new LongWritable(getPos()), line_value);
		
		
		String line = line_value.toString();
		String[] elements = line.split("\t");
		for (int i = 0; i < elements.length; i++) {
			if (i==0) {
				key = new IntWritable(Integer.parseInt(elements[i]));
			}
			
			value.pushBack(Integer.parseInt(elements[i]));
		}
		
		return bool;
	}
	
}


class MatrixInputFormat extends FileInputFormat<IntWritable, IntVector>
implements JobConfigurable {

	private CompressionCodecFactory compressionCodecs = null;

	public void configure(JobConf conf) {
		compressionCodecs = new CompressionCodecFactory(conf);
	}

	protected boolean isSplitable(FileSystem fs, Path file) {
		return compressionCodecs.getCodec(file) == null;
	}

	public RecordReader<IntWritable, IntVector> getRecordReader(
			InputSplit genericSplit, JobConf job,
			Reporter reporter)
			throws IOException {

		reporter.setStatus(genericSplit.toString());
		return new MatrixRecordReader(job, (FileSplit) genericSplit);
	}
}