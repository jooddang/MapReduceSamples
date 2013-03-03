package matrix;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class MatrixMultiplyOutputFormat extends FileOutputFormat<PositionWritable, IntWritable> {
	
	private static final String utf8 = "UTF-8";
	private static final String separator = "\t";
	private static final String newline = "\n";

	@Override
	public RecordWriter<PositionWritable, IntWritable> getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, progress);
		return new MatrixIntermediateRecordWriter(fileOut);
	}

	protected static class MatrixIntermediateRecordWriter implements RecordWriter<PositionWritable, IntWritable> {
		
		protected DataOutputStream out;
		
		public MatrixIntermediateRecordWriter(DataOutputStream out) {
			this.out = out;
		}
		
		@Override
		public void close(Reporter reporter) throws IOException {
			out.close();
		}

		@Override
		public void write(PositionWritable key, IntWritable value)
				throws IOException {
			// Now we need only finalRow and finalCol.
			// Note that those are saved in normal row/col to perform reducer correctly.
			out.write(Integer.toString(key.getRow()).getBytes(utf8));
			out.write(separator.getBytes(utf8));
			out.write(Integer.toString(key.getCol()).getBytes(utf8));
			out.write(separator.getBytes(utf8));
			out.write(Integer.toString(value.get()).getBytes(utf8));
			out.write(newline.getBytes(utf8));
		}
		
	}
}
