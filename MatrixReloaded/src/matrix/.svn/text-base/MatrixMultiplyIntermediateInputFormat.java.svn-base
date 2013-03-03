package matrix;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

public class MatrixMultiplyIntermediateInputFormat extends FileInputFormat<PositionWritable, IntWritable> implements
InputFormat<PositionWritable, IntWritable> {

	@Override
	public RecordReader<PositionWritable, IntWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new MatrixIntermediateRecordReader(job, (FileSplit) split);
	}
	
	protected static class MatrixIntermediateRecordReader implements RecordReader<PositionWritable, IntWritable> {
		private int maxLineLength;
		private long start;
		private long end;
		private LineReader in;
		private long pos;
		
		public MatrixIntermediateRecordReader(JobConf job, FileSplit split)
		throws IOException {

			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;

			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
			if (skipFirstLine) { // skip first line and re-establish "start".
				start += in.readLine(new Text(), 0, (int) Math.min(
						(long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
		}

		@Override
		public void close() throws IOException {
			in.close();
		}

		@Override
		public PositionWritable createKey() {
			return new PositionWritable(); 
		}

		@Override
		public IntWritable createValue() {
			return new IntWritable();
		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public float getProgress() throws IOException {
			return 0;
		}

		@Override
		public boolean next(PositionWritable key, IntWritable value) throws IOException {
			while (pos < end) {

				Text line = new Text();
				int newSize = in.readLine(line, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));
				if (newSize == 0) {
					return false;
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					String lineString = line.toString();
					String[] tokens = lineString.split("\t");
					// finalRow and finalCol of PositionWritable is not included at sorting and calculating hashes,
					// so we put them into normal row/col properties for final addition reduction.
					key.set(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]), -1, -1);
					key.setInputName((tokens[2] == " " ? null : tokens[2]));
					value.set(Integer.parseInt(tokens[3]));
					return true;
				}

				// LOG.info("Skipped line of size " + newSize + " at pos " +
				// (pos - newSize));
			}

			return false;
		}
		
	}

}
