package matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PositionWritable implements WritableComparable<PositionWritable> {
	
	protected int row, col, finalRow, finalCol;
	protected String inputName;
	
	public PositionWritable() {
		this.inputName = null;
		this.row = 0;
		this.col = 0;
		this.finalRow = 0;
		this.finalCol = 0;
	}
	
	public PositionWritable(String inputName, int row, int col, int finalRow, int finalCol) {
		this.inputName = inputName;
		this.row = row;
		this.col = col;
		this.finalRow = finalRow;
		this.finalCol = finalCol;
	}
	
	public void set(int row, int col, int finalRow, int finalCol) {
		this.row = row;
		this.col = col;
		this.finalRow = finalRow;
		this.finalCol = finalCol;
	}
	
	public int getRow() {
		return row;
	}
	
	public int getCol() {
		return col;
	}
	
	public int getFinalRow() {
		return finalRow;		
	}
	
	public int getFinalCol() {
		return finalCol;
	}
	
	public void setInputName(String name) {
		this.inputName = name;
	}
	
	public String getInputName() {
		return inputName;
	}
	
	public void readFields(DataInput in) throws IOException {
		inputName = in.readUTF();
		if (inputName == " ") {
			inputName = null;
		}
		row = in.readInt();
		col = in.readInt();
		finalRow = in.readInt();
		finalCol = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		if (inputName == null) {
			out.writeUTF(" ");
		} else {
			out.writeUTF(inputName);
		}
		out.writeInt(row);
		out.writeInt(col);
		out.writeInt(finalRow);
		out.writeInt(finalCol);
	}

	@Override
	public int compareTo(PositionWritable target) {
		return (this.row < target.row ? -1
				: (this.row == target.row ? (this.col < target.col ? -1
						: (this.col == target.col ? 0 : 1)) : 1));
	}
	
	@Override
	public int hashCode() {
		// Does not consider finalRow, finalCol
		return (row + "," + col).hashCode();
	}
}
