package wtf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Grep {
	public static class Map extends MapReduceBase 
				implements Mapper<LongWritable, Text, NullWritable, Text> {

//		String regexp = "aaaa";
		String regexp;
		
		public void configure(JobConf job){
			super.configure(job);
			regexp = job.get("regexp");
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			
			String line = value.toString();
			System.out.println("map begin " + line);
			 
			if (line.indexOf(regexp) != -1){
				System.out.println("map true");
				output.collect(NullWritable.get(), value);
			} else {
				System.out.println("map false");
			}
		}		
	}
	
	public static class Reduce extends MapReduceBase
				implements Reducer<NullWritable, Text, NullWritable, Text> {

		@Override
		public void reduce(NullWritable key, Iterator<Text> value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			
			System.out.println("Reduce begin");
			
			while (value.hasNext()) {
				System.out.println("Reduce loop ");
				output.collect(NullWritable.get(), value.next());
			}
		}
		
	}
	

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "hdfs://192.168.2.204:9300");
		conf.set("mapred.job.tracker", "192.168.2.204:9301");
		
		// TODO
		conf.set("hadoop.job.ugi", "jooddang,supergroup");
		conf.set("mapred.jar", "c:/hdJooddang.jar");
		
		conf.setMapOutputKeyClass(NullWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("regexp", "modules");
		
		// TODO
		FileInputFormat.setInputPaths(conf, "/user/jooddang/input");
		FileOutputFormat.setOutputPath(conf, new Path("/user/jooddang/outputGrep"));
		
		JobClient client = new JobClient(conf);
		client.submitJob(conf);
	}
}
