package wtf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] words = line.split(" ");
			
			for (String word:words) {
				output.collect(new Text(word), new IntWritable(1));
			}
		}
		
	}



	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int sum = 0;
			while (values.hasNext()){
				sum += values.next().get();
			}
			
			output.collect(key, new IntWritable(sum));
		}
	}
	

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "hdfs://192.168.2.204:9300");
		conf.set("mapred.job.tracker", "192.168.2.204:9301");
		
		// TODO
		conf.set("hadoop.job.ugi", "jooddang,supergroup");
		conf.set("mapred.jar", "c:/a.jar");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("regexp", "aaaa");
		
		// TODO
		FileInputFormat.setInputPaths(conf, "/user/jooddang/input");
		FileOutputFormat.setOutputPath(conf, new Path("/user/jooddang/output"));
		
		JobClient client = new JobClient(conf);
		client.submitJob(conf);
	}
}
