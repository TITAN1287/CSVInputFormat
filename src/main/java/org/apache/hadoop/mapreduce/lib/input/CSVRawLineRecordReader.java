/**
 * Copyright 2015 Tristen Georgiou
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

import java.io.*;

/**
 * This record reader returns a raw CSV row as it's value (as opposed to the CSVLineRecordReader which returns a list
 * of the parsed cells). This reader is more efficient since it doesn't have the added overhead of parsing each cell.
 *
 * @author tristeng (tgeorgiou@phemi.com)
 *
 */
public class CSVRawLineRecordReader extends CSVRecordReader<LongWritable, Text> {
    private char delimiter;
    private StringBuilder sb;

    public CSVRawLineRecordReader() {}

    /**
     * Constructor used to manually set a stream and configuration (mostly for testing)
     * @param is the input stream
     * @param conf the configuration
     * @throws IOException
     */
    public CSVRawLineRecordReader(InputStream is, Configuration conf) throws IOException {
        init(is, conf);
    }

    /**
     * Initializes this record reader
     * @param is the input stream for the CSV file
     * @param conf the job configuration
     * @throws IOException
     */
    @Override
    public void init(InputStream is, Configuration conf) throws IOException {
        String delimiter = conf.get(CSVFileInputFormat.FORMAT_DELIMITER,
                CSVFileInputFormat.DEFAULT_DELIMITER);
        if (delimiter.length() != 1) {
            throw new IOException("The delimiter can only be a single character.");
        }
        this.delimiter = delimiter.charAt(0);
        in = getReader(is, conf);
        this.sb = new StringBuilder();
    }

    @Override
    protected void initKeyAndValue() {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
    }

    /**
     * Reads a raw CSV Line from a file; note that this algorithm is simpler than the {@link CSVLineRecordReader} since
     * it doesn't care about content within a record, it simply cares about finding the end of a record
     * @param csvRow used to store the raw CSV line
     * @return the number of bytes read
     * @throws IOException
     */
    @Override
    protected int readLine(Text csvRow) throws IOException {
        char c;
        int numRead = 0;
        boolean insideQuote = false;
        // Empty string buffer
        sb.setLength(0);
        int i;
        // Reads each char from input stream unless eof was reached
        while ((i = in.read()) != -1) {
            // it is very important this value reflects the exact number of bytes read, otherwise the CSVTextInputFormat
            // getSplits() function would break
            numRead = bytesReadForCharacter(i, numRead);
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
                insideQuote = !insideQuote;
            }
            // Check delimiters, but only those outside of quotes
            if (!insideQuote) {
                // A new line outside of a quote is a real csv line breaker
                if (c == '\n' || c == '\r') {
                    break;
                }
            }
        }
        // remove trailing LF or CR
        int lastIndex = sb.length() - 1;
        if (sb.length() > 0 && (sb.charAt(lastIndex) == '\n' || sb.charAt(lastIndex) == '\r')) {
            sb.deleteCharAt(lastIndex);
        }
        // if we hit eof and read some bytes but our buffer is empty, return that we read 0
        // if we don't the input format will emit another record, but it will be empty
        if (i == -1 && sb.length() == 0) {
            return 0;
        }
        csvRow.set(sb.toString().getBytes("UTF-8"));
        return numRead;
    }
}
