/**
 * Copyright 2014 Marcelo Elias Del Valle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Reads a CSV line. CSV files could be multiline, as they may have line breaks
 * inside a column
 * 
 * @author mvallebr, tristeng
 *
 * October, 2015: tristeng (tgeorgiou@phemi.com) updates to restrict delimiter and separator to single character,
 * support for carriage returns, support for UTF-8 multi-byte characters, support for unescaping double delimiters.
 * Also pushed much of the functionality into a parent class.
 * 
 */
public class CSVLineRecordReader extends CSVRecordReader<LongWritable, List<Text>> {
    private char delimiter;
    private char separator;
    private StringBuilder sb;

    /**
     * Default constructor is needed when called by reflection from hadoop
     *
     */
    public CSVLineRecordReader() {}

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
    @Override
    public void init(InputStream is, Configuration conf) throws IOException {
        String delimiter = conf.get(CSVFileInputFormat.FORMAT_DELIMITER,
                CSVFileInputFormat.DEFAULT_DELIMITER);
        if (delimiter.length() != 1) {
            throw new IOException("CSVLineRecordReader: The delimiter can only be a single character.");
        }
        this.delimiter = delimiter.charAt(0);
        String separator = conf.get(CSVFileInputFormat.FORMAT_SEPARATOR,
                CSVFileInputFormat.DEFAULT_SEPARATOR);
        if (separator.length() != 1) {
            throw new IOException("CSVLineRecordReader: The separator can only be a single character.");
        }
        if (delimiter.equals(separator)) {
            throw new IOException("CSVLineRecordReader: delimiter and separator cannot be the same character");
        }
        this.separator = separator.charAt(0);
        this.in = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        this.sb = new StringBuilder();
    }

    @Override
    protected void initKeyAndValue() {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new ArrayListTextWritable();
        }
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
    @Override
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
        int lastIndex = sb.length()-1;
        if (sb.length() > 0 && (sb.charAt(lastIndex) == '\n' || sb.charAt(lastIndex) == '\r')) {
            sb.deleteCharAt(lastIndex);
            lastIndex--;
        }
        // NOTE: it's possible that this cell ends in a separator, which is why we don't auto-remove it
        if (removeSeparator) {
            sb.delete(lastIndex, sb.length());
            lastIndex--;
        }
        if (sb.length() > 1 && sb.charAt(0) == delimiter && sb.charAt(lastIndex) == delimiter) {
            sb.delete(lastIndex, sb.length());
            sb.delete(0, 1);
        }
        values.add(new Text(sb.toString().getBytes("UTF-8")));
        // Empty string buffer
        sb.setLength(0);
    }
}
