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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

/**
 * Base clase for reading CSV records
 *
 * @author tristeng (tgeorgiou@phemi.com)
 *
 * @param <K> the key to emit
 * @param <V> the value to emit
 */
public abstract class CSVRecordReader<K, V> extends RecordReader<K, V> {
    private static final Log LOG = LogFactory.getLog(CSVRecordReader.class);

    protected long start;
    protected long pos;
    protected long end;
    protected Reader in;
    protected K key = null;
    protected V value = null;
    protected boolean isAscii;

    /**
     * Initialize function called by initialize and constructors
     *
     * @param is the input stream for the CSV file
     * @param conf the job configuration
     * @throws IOException
     */
    public abstract void init(InputStream is, Configuration conf) throws IOException;

    /**
     * The function that reads a CSV line and updates the value
     *
     * @param value the value to be updated
     * @return the number of bytes read
     * @throws IOException
     */
    protected abstract int readLine(V value) throws IOException;

    /**
     * Use this function to properly initialize the key and value; called by nextKeyValue()
     *
     */
    protected abstract void initKeyAndValue();

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        isAscii = false; // our default is UTF-8...
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
            throw new IOException("CSVRecordReader does not support compression.");
        } else {
            fileIn.seek(start);
        }

        pos = start;
        init(fileIn, job);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        // we should have an exact split on a CSV line, so if the last call read a line we should have pos == end, so
        // we shouldn't read any more since the next split will get it
        if (pos >= end) {
            key = null;
            value = null;
            return false;
        }
        initKeyAndValue();
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

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
    }

    protected Reader getReader(InputStream is, Configuration conf) throws UnsupportedEncodingException {
        String encoding = conf.get(CSVFileInputFormat.FORMAT_ENCODING, CSVFileInputFormat.DEFAULT_ENCODING);
        boolean isEncodingSupported = false;
        for (String supportedEncoding : CSVTextInputFormat.SUPPORTED_ENCODINGS) {
            if (supportedEncoding.equals(encoding)) {
                isEncodingSupported = true;
                break;
            }
        }
        if (!isEncodingSupported) {
            StringBuilder errSb = new StringBuilder();
            for (String supportedEncoding : CSVTextInputFormat.SUPPORTED_ENCODINGS) {
                if (errSb.length() != 0) {
                    errSb.append(", ");
                }
                errSb.append("'");
                errSb.append(supportedEncoding);
                errSb.append("'");
            }
            throw new UnsupportedEncodingException("CSVInputFormat only supports the following encodings: " +
                    errSb.toString());
        }
        if (encoding.equals("ISO-8859-1")) {
            isAscii = true;
        }
        CharsetDecoder decoder = Charset.forName(encoding).newDecoder();
        // NOTE: we are very strict about encoded characters since if we replace or ignore, we may unwittingly mess up
        //       our split points...the reader doesn't tell us how many bytes were malformed/unmappable
        decoder.onMalformedInput(CodingErrorAction.REPORT);
        decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        return new BufferedReader(new InputStreamReader(is, decoder));
    }

    protected int bytesReadForCharacter(int i, int numRead) {
        numRead++;
        if (!isAscii) {
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
        }
        return numRead;
    }
}
