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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVTextInputFormatTest {

    @Test
    public void testSmallFileSplit() throws Exception {
        Configuration conf = createConfig("./fixtures/bull.csv");
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, ";");
        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
        assertEquals(1, actualSplits.size());
    }

    @Test
    public void testSplits() throws Exception {
        // same test as above, but let's force this file to split
        long splitSize = 1024;
        Configuration conf = createConfig("./fixtures/bull.csv"); // this file is 4640
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, ";");
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, splitSize);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
        assertEquals(5, actualSplits.size());

        // let's make sure our last split has sane lines - the last split has the last 3 lines of the file
        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(4), context);

        recordReader.initialize(actualSplits.get(4), context);

        recordReader.nextKeyValue();
        List<Text> line1 = recordReader.getCurrentValue();

        assertEquals(15, line1.size());
        assertEquals("38", line1.get(0).toString());
        assertEquals("22833930510", line1.get(1).toString());
        assertEquals("Fernanda", line1.get(2).toString());
        assertEquals("Santos", line1.get(3).toString());
        assertEquals("19/03/85", line1.get(4).toString());
        assertEquals("22/02/13", line1.get(5).toString());
        assertEquals("E", line1.get(6).toString());
        assertEquals("Inadimplente", line1.get(7).toString());
        assertEquals("21/03/13", line1.get(8).toString());
        assertEquals("1,9", line1.get(9).toString());
        assertEquals("92", line1.get(10).toString());
        assertEquals("Negro", line1.get(11).toString());
        assertEquals("Sorocaba", line1.get(12).toString());
        assertEquals("FIAT", line1.get(13).toString());
        assertEquals("2010", line1.get(14).toString());

        recordReader.nextKeyValue();
        List<Text> line2 = recordReader.getCurrentValue();

        assertEquals(15, line2.size());

        recordReader.nextKeyValue();
        List<Text> line3 = recordReader.getCurrentValue();

        assertEquals(15, line3.size());

        // we shouldn't have any more lines to process
        assertEquals(false, recordReader.nextKeyValue());
    }

    @Test
    public void testLatin1Encoding() throws Exception {
        long splitSize = 512;
        Configuration conf = createConfig("./fixtures/latin1.csv");
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, splitSize);
        conf.set(CSVTextInputFormat.FORMAT_ENCODING, "ISO-8859-1");
        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
        assertEquals(4, actualSplits.size());
    }

    @Test(expected = CharacterCodingException.class)
    public void testMisMatchedEncoding() throws Exception {
        // input file has latin1 characters from the extended ascii character set, but these are not valid when using a
        // UTF-8 encoding
        long splitSize = 1024;
        Configuration conf = createConfig("./fixtures/latin1.csv");
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, splitSize);
        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
    }

    @Test(expected = UnsupportedEncodingException.class)
    public void testUnsupportedEncoding() throws Exception {
        Configuration conf = createConfig("./fixtures/latin1.csv");
        conf.set(CSVTextInputFormat.FORMAT_ENCODING, "UTF-16");
        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
    }

    @Test
    public void testEmptyDelimitedCells() throws Exception {
        Configuration conf = createConfig("./fixtures/empty_cells.csv");
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVTextInputFormat inputFormat = new CSVTextInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
        assertEquals(1, actualSplits.size());

        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(0), context);

        recordReader.initialize(actualSplits.get(0), context);

        recordReader.nextKeyValue();
        List<Text> line = recordReader.getCurrentValue();

        assertEquals(3, line.size());
        assertEquals("col1", line.get(0).toString());
        assertEquals("col2", line.get(1).toString());
        assertEquals("col3", line.get(2).toString());

        recordReader.nextKeyValue();
        line = recordReader.getCurrentValue();
        assertEquals(3, line.size());
        assertEquals("", line.get(0).toString());
        assertEquals("", line.get(1).toString());
        assertEquals("", line.get(2).toString());

        recordReader.nextKeyValue();
        line = recordReader.getCurrentValue();
        assertEquals(3, line.size());
        assertEquals("abc", line.get(0).toString());
        assertEquals("", line.get(1).toString());
        assertEquals("", line.get(2).toString());

        recordReader.nextKeyValue();
        line = recordReader.getCurrentValue();
        assertEquals(3, line.size());
        assertEquals("", line.get(0).toString());
        assertEquals("abc", line.get(1).toString());
        assertEquals("", line.get(2).toString());

        recordReader.nextKeyValue();
        line = recordReader.getCurrentValue();
        assertEquals(3, line.size());
        assertEquals("", line.get(0).toString());
        assertEquals("", line.get(1).toString());
        assertEquals("abc", line.get(2).toString());
    }

    private Configuration createConfig(String fileName) {
        Configuration conf = new Configuration();

        conf.setStrings("mapred.input.dir", fileName);
        conf.set(CSVFileInputFormat.FORMAT_DELIMITER, "\"");
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, ",");

        return conf;
    }
}
