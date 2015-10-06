package org.apache.hadoop.mapreduce.lib.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Reads a CSV line. CSV files could be multiline, as they may have line breaks
 * inside a column
 * 
 * @author mvallebr, tristen
 *
 * October, 2015: tristeng (tgeorgiou@phemi.com) updates to restrict delimiter and separator to single character,
 * support for carriage returns, support for UTF-8 multi-byte characters, support for unescaping double delimiters
 * 
 */
public class CSVLineRecordReader extends RecordReader<LongWritable, List<Text>> {
	private static final Log LOG = LogFactory.getLog(CSVLineRecordReader.class);
	public static final String FORMAT_DELIMITER = "mapreduce.csvinput.delimiter";
	public static final String FORMAT_SEPARATOR = "mapreduce.csvinput.separator";
	public static final String DEFAULT_DELIMITER = "\"";
	public static final String DEFAULT_SEPARATOR = ",";

	private long start;
	private long pos;
	private long end;
	protected Reader in;
	private LongWritable key = null;
	private List<Text> value = null;
	private char delimiter;
	private char separator;
	private StringBuilder sb;

	/**
	 * Default constructor is needed when called by reflection from hadoop
	 * 
	 */
	public CSVLineRecordReader() {
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
	public CSVLineRecordReader(InputStream is, Configuration conf) throws IOException {
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
		String delimiter = conf.get(FORMAT_DELIMITER, DEFAULT_DELIMITER);
		if (delimiter.length() != 1) {
			throw new IOException("The delimiter can only be a single character.");
		}
		this.delimiter = delimiter.charAt(0);
		String separator = conf.get(FORMAT_SEPARATOR, DEFAULT_SEPARATOR);
		if (separator.length() != 1) {
			throw new IOException("The separator can only be a single character.");
		}
		this.separator = separator.charAt(0);
		this.in = new BufferedReader(new InputStreamReader(is, "UTF-8"));
		this.sb = new StringBuilder();
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
	protected int readLine(List<Text> values) throws IOException {
		values.clear(); // Empty value columns list
		char c;
		int numRead = 0;
		boolean insideQuote = false;
		// Empty string buffer
		sb.setLength(0);
		int i;
		boolean lastCharWasDelimiter = false;
		// Reads each char from input stream unless eof was reached
		while ((i = in.read()) != -1) {
			// it is very important this value reflects the exact number of bytes read, otherwise the CSVTextInputFormat
			// getSplits() function would break
			numRead++;
			// if we read a utf-8 character, we need to account for it's size in bytes
			// see https://en.wikipedia.org/wiki/UTF-8 (5 and 6 byte characters are no longer part of utf-8, RFC 3629)
			if (i > 0x007F) { // 127
				numRead++;
			}
			if (i > 0x07FF) { // 2047
				numRead++;
			}
			if (i > 0xFFFF) { // 65535
				numRead++;
			}
			c = (char) i;
			// if our buffer is empty and we encounter a linefeed or carriage return, it likely means the file uses
			// both, and we just finished the previous line on one or the other, so just ignore it until we get some
			// content
			if (sb.length() == 0 && (c == '\n' || c == '\r')) {
				continue;
			}
			sb.append(c);
			// Check quotes, as delimiter inside quotes don't count
			if (c == delimiter) {
				// if the last character was a delimiter AND the current character is a delimiter, delete the last
				// character as it is an escape character (i.e. when someone wants a double quote in their value,
				// it becomes escaped with another double quote, if the real value is 'a string with "double quotes"'
				// it becomes "a string with ""double quotes""" in a csv file)
				if (lastCharWasDelimiter) {
					lastCharWasDelimiter = false;
					sb.deleteCharAt(sb.length() - 1);
				} else {
					lastCharWasDelimiter = true;
				}
				insideQuote = !insideQuote;
			} else {
				lastCharWasDelimiter = false;
			}
			// Check delimiters, but only those outside of quotes
			if (!insideQuote) {
				if (c == separator) {
					addCell(values, true);
				}
				// A new line outside of a quote is a real csv line breaker
				if (c == '\n' || c == '\r') {
					break;
				}
			}
		}
		addCell(values, false);
		return numRead;
	}

	/**
	 * Helper function that adds a new value to the values list passed as
	 * argument.
	 * 
	 * @param values
	 *            values list
	 * @param removeSeparator
	 *            should be true when called in the middle of the line, when a
	 *            delimiter was found, and false when sb contains the line
	 *            ending
	 * @throws UnsupportedEncodingException
	 */
	protected void addCell(List<Text> values, boolean removeSeparator) throws UnsupportedEncodingException {
        // remove trailing LF or CR
        if (sb.length() > 0 && (sb.charAt(sb.length()-1) == '\n' || sb.charAt(sb.length()-1) == '\r')) {
            sb.deleteCharAt(sb.length()-1);
        }
		// NOTE: it's possible that this cell ends in a separator, which is why we don't auto-remove it
		if (removeSeparator) {
			sb.delete(sb.length() - 1, sb.length());
		}
		if (sb.length() > 1 && sb.charAt(0) == delimiter && sb.charAt(sb.length()-1) == delimiter) {
			sb.delete(sb.length() - 1, sb.length());
			sb.delete(0, 1);
		}
		values.add(new Text(sb.toString().getBytes("UTF-8")));
		// Empty string buffer
		sb.setLength(0);
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

		// open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);

		CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
		if (null!=codec) {
			LOG.error("CSVLineRecordReader does not support compression.");
			throw new IOException("CSVLineRecordReader does not support compression.");
		} else {
			fileIn.seek(start);
		}

		pos = start;
		init(fileIn, job);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	public boolean nextKeyValue() throws IOException {
		// we should have an exact split on a CSV line, so if the last call read a line we should have pos == end, so
		// we shouldn't read any more since the next split will get it
		if (pos >= end) {
			key = null;
			value = null;
			return false;
		}
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new ArrayListTextWritable();
		}
		int size = readLine(value);
		pos += size;
		// size is 0 if EOF, although we should have exact splits...
		if (size == 0) {
			LOG.warn(String.format("Encountered EOF but expected to end on exact split boundary! pos=%d, start=%d, " +
					"end=%d", pos, start, end));
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
	public List<Text> getCurrentValue() {
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
	}
}
