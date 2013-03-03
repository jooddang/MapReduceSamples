package matrix;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class MatrixOutputFormat extends FileOutputFormat<IntWritable, IntVectorWritable> {
	
	private static final String utf8 = "UTF-8";
	private static final String separator = "\t";
	private static final String newline = "\n";

	@Override
	public RecordWriter<IntWritable, IntVectorWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		// Copied from the TextOutputFormat class.
		boolean isCompressed = getCompressOutput(job);
	    if (!isCompressed) {
	      Path file = FileOutputFormat.getTaskOutputPath(job, name);
	      FileSystem fs = file.getFileSystem(job);
	      FSDataOutputStream fileOut = fs.create(file, progress);
	      return new MatrixRowRecordWriter(fileOut);
	    } else {
	      Class<? extends CompressionCodec> codecClass =
	        getOutputCompressorClass(job, GzipCodec.class);
	      // create the named codec
	      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
	      // build the filename including the extension
	      Path file = 
	        FileOutputFormat.getTaskOutputPath(job, 
	                                           name + codec.getDefaultExtension());
	      FileSystem fs = file.getFileSystem(job);
	      FSDataOutputStream fileOut = fs.create(file, progress);
	      return new MatrixRowRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
	    }
	}
	
	protected static class MatrixRowRecordWriter implements RecordWriter<IntWritable, IntVectorWritable> {
		
		protected DataOutputStream out;
		
		public MatrixRowRecordWriter(DataOutputStream out) {
			this.out = out;
		}
		
		@Override
		public void close(Reporter reporter) throws IOException {
			out.close();
		}

		@Override
		public void write(IntWritable key, IntVectorWritable value)
				throws IOException {
			out.write(key.toString().getBytes(utf8));
			out.write(separator.getBytes(utf8));
			value.rewind();
			while (value.hasNext()) {
				out.write(Integer.toString(value.next()).getBytes(utf8));
				if (value.hasNext()) {
					out.write(separator.getBytes(utf8));
				}
			}
			out.write(newline.getBytes(utf8));
		}
		
	}

}
