package matrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatrixAdder extends Configured implements Tool {
	
	public static class Map extends MapReduceBase implements Mapper<PositionWritable, IntVectorWritable, IntWritable, IntVectorWritable> {

		@Override
		public void map(PositionWritable key, IntVectorWritable value,
				OutputCollector<IntWritable, IntVectorWritable> output,
				Reporter reporter) throws IOException {
			
			// We don't need the input file name for matrix addition.
			output.collect(new IntWritable(key.getRow()), value);
			
		}	
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntVectorWritable, IntWritable, IntVectorWritable> {

		@Override
		public void reduce(IntWritable key, Iterator<IntVectorWritable> values,
				OutputCollector<IntWritable, IntVectorWritable> output,
				Reporter reporter) throws IOException {

			int max = 0;
			ArrayList<IntVectorWritable> valuesClone = new ArrayList<IntVectorWritable>();
			while (values.hasNext()) {
				valuesClone.add(values.next());
				
				if (valuesClone.get(valuesClone.size() - 1).getSize() > max) {
					max = valuesClone.get(valuesClone.size() - 1).getSize(); 
				}
			}
			
			int[] sum = new int[max];
			for (IntVectorWritable line:valuesClone) {
				line.rewind();
				while (line.hasNext()) {
					sum[line.getCurrentPos()] += line.next();
				}
			}
			
			output.collect(key, new IntVectorWritable(sum));
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println("Initializing MatrixAdder...");
		JobConf conf = new JobConf(getConf(), MatrixAdder.class);
		conf.set("fs.default.name", "hdfs://192.168.2.204:9300");
		conf.set("hadoop.job.ugi", "jooddang,supergroup");
		conf.set("mapred.job.tracker", "192.168.2.204:9301");
		conf.set("mapred.jar", "c:/matrix.jar");
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntVectorWritable.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntVectorWritable.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(MatrixInputFormat.class);
		conf.setOutputFormat(MatrixOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, "/user/daybreaker/matrices/matrix-100x100-0,/user/daybreaker/matrices/matrix-100x100-1," +
				"/user/daybreaker/matrices/matrix-100x100-2," +
				"/user/daybreaker/matrices/matrix-100x100-3," +
				"/user/daybreaker/matrices/matrix-1000x1000-0," +
				"/user/daybreaker/matrices/matrix-1000x1000-1," +
				"/user/daybreaker/matrices/matrix-1000x1000-2");
		FileOutputFormat.setOutputPath(conf, new Path("/user/jooddang/outputMatrix1"));
		
		System.out.println("Submitting the job...");
		JobClient client = new JobClient(conf);
		client.submitJob(conf);
		System.out.println("Job is submitted.");
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MatrixAdder(), args);
	}

}
