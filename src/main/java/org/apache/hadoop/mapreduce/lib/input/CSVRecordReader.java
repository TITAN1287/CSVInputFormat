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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

public abstract class CSVRecordReader<K, V> extends RecordReader<K, V> {
    private static final Log LOG = LogFactory.getLog(CSVRecordReader.class);

    protected long start;
    protected long pos;
    protected long end;
    protected Reader in;
    protected K key = null;
    protected V value = null;

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
}
