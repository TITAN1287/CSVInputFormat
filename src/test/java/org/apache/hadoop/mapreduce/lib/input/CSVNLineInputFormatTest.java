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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVNLineInputFormatTest {

    @Before
    public void setup() {
    }

    @Test
    public void shouldGenerateRightSplitsForOneByteEncodedSymbols() throws Exception {
        Configuration conf = createConfig("./fixtures/teste2.csv");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 1);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));

        assertEquals(3, actualSplits.size());

        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(1), context);

        recordReader.initialize(actualSplits.get(1), context);

        recordReader.nextKeyValue();
        List<Text> secondLineValue = recordReader.getCurrentValue();

        assertEquals("Jim Sample", secondLineValue.get(0).toString());
        assertEquals("", secondLineValue.get(1).toString());
        assertEquals("jim@sample.com", secondLineValue.get(2).toString());
    }

    @Test
    public void shouldGenerateRightSplitsForTwoByteEncodedSymbols() throws Exception {
        Configuration conf = createConfig("./fixtures/teste2_cyrillic.csv");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 1);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));

        assertEquals(3, actualSplits.size());

        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(1), context);

        recordReader.initialize(actualSplits.get(1), context);

        recordReader.nextKeyValue();
        List<Text> secondLineValue = recordReader.getCurrentValue();

        assertEquals("Джим Сэмпл", secondLineValue.get(0).toString());
        assertEquals("", secondLineValue.get(1).toString());
        assertEquals("jim@sample.com", secondLineValue.get(2).toString());
    }

    @Test
    public void shouldReturnListsAsRecords() throws Exception {
        Configuration conf = createConfig("./fixtures/teste2.csv");
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));

        assertEquals(1, actualSplits.size());

        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(0), context);

        recordReader.initialize(actualSplits.get(0), context);

        recordReader.nextKeyValue();
        List<Text> firstLineValue = recordReader.getCurrentValue();

        assertEquals("Joe Demo", firstLineValue.get(0).toString());
        assertEquals("2 Demo Street,\nDemoville,\nAustralia. 2615", firstLineValue.get(1).toString());
        assertEquals("joe@someaddress.com", firstLineValue.get(2).toString());

        recordReader.nextKeyValue();
        List<Text> secondLineValue = recordReader.getCurrentValue();

        assertEquals("Jim Sample", secondLineValue.get(0).toString());
        assertEquals("", secondLineValue.get(1).toString());
        assertEquals("jim@sample.com", secondLineValue.get(2).toString());

        recordReader.nextKeyValue();
        List<Text> thirdLineValue = recordReader.getCurrentValue();

        assertEquals("Jack Example", thirdLineValue.get(0).toString());
        assertEquals("1 Example Street, Exampleville, Australia.\n2615", thirdLineValue.get(1).toString());
        assertEquals("jack@example.com", thirdLineValue.get(2).toString());
    }

    @Test
    public void shouldReturnListsAsRecordsForTwoBytesStrings() throws Exception {
        Configuration conf = createConfig("./fixtures/teste2_cyrillic.csv");
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));

        assertEquals(1, actualSplits.size());

        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(0), context);

        recordReader.initialize(actualSplits.get(0), context);

        recordReader.nextKeyValue();
        List<Text> firstLineValue = recordReader.getCurrentValue();

        assertEquals("Джо Демо", firstLineValue.get(0).toString());
        assertEquals("2 улица Демо,\nДемовиль,\nАвстралия. 2615", firstLineValue.get(1).toString());
        assertEquals("joe@someaddress.com", firstLineValue.get(2).toString());

        recordReader.nextKeyValue();
        List<Text> secondLineValue = recordReader.getCurrentValue();

        assertEquals("Джим Сэмпл", secondLineValue.get(0).toString());
        assertEquals("", secondLineValue.get(1).toString());
        assertEquals("jim@sample.com", secondLineValue.get(2).toString());

        recordReader.nextKeyValue();
        List<Text> thirdLineValue = recordReader.getCurrentValue();

        assertEquals("Джек Экзампл", thirdLineValue.get(0).toString());
        assertEquals("1 улица Экзампл, Экзамплвиль, Австралия.\n2615", thirdLineValue.get(1).toString());
        assertEquals("jack@example.com", thirdLineValue.get(2).toString());
    }

    @Test(expected = IOException.class)
    public void testBadSeparator() throws Exception {
        Configuration conf = createConfig("./fixtures/teste2.csv");
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, "abcd");
        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        InputSplit split = new FileSplit(new Path("/some/file.csv"), 0, 10, new String[0]);
        inputFormat.createRecordReader(split, context);
    }

    @Test(expected = IOException.class)
    public void testBadDelimiter() throws Exception {
        Configuration conf = createConfig("./fixtures/teste2.csv");
        conf.set(CSVFileInputFormat.FORMAT_DELIMITER, "abcd");
        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
    }

    @Test
    public void testSetFunction() throws Exception {
        Job job = Job.getInstance();
        CSVNLineInputFormat.setNumLinesPerSplit(job, 10);
        assertEquals(10, CSVNLineInputFormat.getNumLinesPerSplit(job));
    }

    private Configuration createConfig(String fileName) {
        Configuration conf = new Configuration();

        conf.setStrings("mapred.input.dir", fileName);
        conf.set(CSVFileInputFormat.FORMAT_DELIMITER, "\"");
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, ",");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);

        return conf;
    }
}
