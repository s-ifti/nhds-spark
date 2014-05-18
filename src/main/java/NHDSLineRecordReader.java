package sifti.nhds.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.zip.ZipInputStream;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * Reads a CSV line. CSV files could be multiline, as they may have line breaks
 * inside a column
 * 
 * @author mvallebr
 * 
 */
public class NHDSLineRecordReader extends RecordReader<LongWritable, BSONObject> {


	private long start;
	private long pos;
	private long end;
	protected Reader in;
	private InputStream is;
	private LongWritable key = null;
	private BasicBSONObject value = null;


	/**
	 * Default constructor is needed when called by reflection from hadoop
	 * 
	 */
	public NHDSLineRecordReader() {
	}

	/**
	 * Constructor to be called from FileInputFormat.createRecordReader
	 * 
	 * @param is
	 *            - the input stream
	 * @param conf
	 *            - hadoop conf
	 * @throws IOException
	 */
	public NHDSLineRecordReader(InputStream is, Configuration conf) throws IOException {
		init(is, conf);
	}

	/**
	 * reads configuration set in the runner, setting delimiter and separator to
	 * be used to process the CSV file . If isZipFile is set, creates a
	 * ZipInputStream on top of the InputStream
	 * 
	 * @param is
	 *            - the input stream
	 * @param conf
	 *            - hadoop conf
	 * @throws IOException
	 */
	public void init(InputStream is, Configuration conf) throws IOException {

		this.is = is;
		this.in = new BufferedReader(new InputStreamReader(is));
	}

	/**
	 * Parses a line from the CSV, from the current stream position. It stops
	 * parsing when it finds a new line char outside two delimiters
	 * 
	 * @param values
	 *            List of column values parsed from the current CSV line
	 * @return number of chars processed from the stream
	 * @throws IOException
	 */
	protected int readLine( BasicBSONObject values) throws IOException {
		values.clear();// Empty value columns list
		char c;
		int numRead = 0;
		boolean insideQuote = false;
		StringBuffer sb = new StringBuffer();
		int i;
		int quoteOffset = 0, delimiterOffset = 0;
		// Reads each char from input stream unless eof was reached
		while ((i = in.read()) != -1) {
			c = (char) i;
			numRead++;
			sb.append(c);

			if (c == '\n') {
				//System.out.println("LINE: " + sb.toString());
				values.put("text", sb.toString());
				break;
			}
		}
		return numRead;
	}

	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop
	 * .mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();

		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
//		compressionCodecs = new CompressionCodecFactory(job);
//		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

/*		if (codec != null) {
			is = codec.createInputStream(fileIn);
			end = Long.MAX_VALUE;
		} else {
*/			
			if (start != 0) {
				fileIn.seek(start);
			}
			is = fileIn;
//		}

		this.pos = start;
		init(is, job);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);

		if (value == null) {
			value = new BasicBSONObject();
		}
		if (pos >= end)
			return false;
		int newSize = 0;
		newSize = readLine(value);
		pos += newSize;
		if (newSize == 0) {
			
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public BSONObject getCurrentValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
			in = null;
		}
		if (is != null) {
			is.close();
			is = null;
		}
	}
}
