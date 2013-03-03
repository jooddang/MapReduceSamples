package matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

@SuppressWarnings("deprecation")
public class MatrixInputFormat extends
		FileInputFormat<PositionWritable, IntVectorWritable> implements
		InputFormat<PositionWritable, IntVectorWritable> {

	private static final double SPLIT_SLOP = 1.1; // 10% slop
	private long minSplitSize = 1;

	@Override
	public RecordReader<PositionWritable, IntVectorWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new MatrixRowRecordReader(job, (InputAwareFileSplit) split);
	}

	/**
	 * Splits files returned by {@link #listStatus(JobConf)} when they're too big.
	 * This method is modified to pass the original input file name to splits.
	 */
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		FileStatus[] files = listStatus(job);

		long totalSize = 0; // compute total size
		for (FileStatus file : files) { // check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: " + file.getPath());
			}
			totalSize += file.getLen();
		}

		long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
		long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
				minSplitSize);

		// generate splits
		ArrayList<InputAwareFileSplit> splits = new ArrayList<InputAwareFileSplit>(
				numSplits);
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job);
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(fs, path)) {
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(goalSize, minSize, blockSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new InputAwareFileSplit(path, length
							- bytesRemaining, splitSize, blkLocations[blkIndex]
							.getHosts(), path.getName()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new InputAwareFileSplit(path, length
							- bytesRemaining, bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts(),
							path.getName()));
				}
			} else if (length != 0) {
				splits.add(new InputAwareFileSplit(path, 0, length,
						blkLocations[0].getHosts(), path.getName()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new InputAwareFileSplit(path, 0, length,
						new String[0], path.getName()));
			}
		}
		LOG.debug("Total # of splits: " + splits.size());
		return splits.toArray(new InputAwareFileSplit[splits.size()]);
	}

	protected static class InputAwareFileSplit implements InputSplit {
		// Copied from FileSplit, but could not extend it because member
		// variables were private.
		private Path file;
		private long start;
		private long length;
		private String[] hosts;
		private String inputName;

		InputAwareFileSplit() {
		}

		/**
		 * Constructs a split with host information
		 * 
		 * @param file
		 *            the file name
		 * @param start
		 *            the position of the first byte in the file to process
		 * @param length
		 *            the number of bytes in the file to process
		 * @param hosts
		 *            the list of hosts containing the block, possibly null
		 * @param inputName
		 *            the name of the original input file name
		 */
		public InputAwareFileSplit(Path file, long start, long length,
				String[] hosts, String inputName) {
			this.file = file;
			this.start = start;
			this.length = length;
			this.hosts = hosts;
			this.inputName = inputName;
		}

		/** The file containing this split's data. */
		public Path getPath() {
			return file;
		}

		/** The position of the first byte in the file to process. */
		public long getStart() {
			return start;
		}

		/** The number of bytes in the file to process. */
		public long getLength() {
			return length;
		}

		public String getInputName() {
			return inputName;
		}

		public String toString() {
			return file + ":" + start + "+" + length;
		}

		// //////////////////////////////////////////
		// Writable methods
		// //////////////////////////////////////////

		public void write(DataOutput out) throws IOException {
			out.writeUTF(inputName);
			UTF8.writeString(out, file.toString());
			out.writeLong(start);
			out.writeLong(length);
		}

		public void readFields(DataInput in) throws IOException {
			inputName = in.readUTF();
			file = new Path(UTF8.readString(in));
			start = in.readLong();
			length = in.readLong();
			hosts = null;
		}

		public String[] getLocations() throws IOException {
			if (this.hosts == null) {
				return new String[] {};
			} else {
				return this.hosts;
			}
		}

	}

	protected static class MatrixRowRecordReader implements
			RecordReader<PositionWritable, IntVectorWritable> {

		private int maxLineLength;
		private long start;
		private long end;
		private CompressionCodecFactory compressionCodecs;
		private LineReader in;
		private long pos;
		private String inputName;

		public MatrixRowRecordReader(JobConf job, InputAwareFileSplit split)
				throws IOException {

			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;

			if (codec != null) {
				in = new LineReader(codec.createInputStream(fileIn), job);
				end = Long.MAX_VALUE;
			} else {
				if (start != 0) {
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new LineReader(fileIn, job);
			}
			if (skipFirstLine) { // skip first line and re-establish "start".
				start += in.readLine(new Text(), 0, (int) Math.min(
						(long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
			this.inputName = split.getInputName();
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
		public IntVectorWritable createValue() {
			return new IntVectorWritable();
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
		public boolean next(PositionWritable key, IntVectorWritable value)
				throws IOException {

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
					key.set(Integer.parseInt(tokens[0]), -1, -1, -1);
					key.setInputName(inputName);
					value.setSize(tokens.length - 1);
					value.rewind();
					for (int index = 1; index < tokens.length; index++) {
						System.out.println("MatrixInput ("+inputName+"): row="+tokens[0]+", col="+(index-1)+", val="+tokens[index]);
						value.push(Integer.parseInt(tokens[index]));
					}
					value.rewind();
					return true;
				}

				// LOG.info("Skipped line of size " + newSize + " at pos " +
				// (pos - newSize));
			}

			return false;
		}

	}
}
