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

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVRawTextInputFormatTest {

    @Test
    public void testSmallFileSplit() throws Exception {
        Configuration conf = createConfig("./fixtures/bull.csv");
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, ";");
        CSVRawTextInputFormat inputFormat = new CSVRawTextInputFormat();
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

        CSVRawTextInputFormat inputFormat = new CSVRawTextInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()));
        assertEquals(5, actualSplits.size());

        // let's make sure our last split has sane lines - the last split has the last 3 lines of the file
        RecordReader<LongWritable, Text> recordReader =
                inputFormat.createRecordReader(actualSplits.get(4), context);

        recordReader.initialize(actualSplits.get(4), context);

        recordReader.nextKeyValue();
        Text line1 = recordReader.getCurrentValue();

        assertEquals("38;22833930510;Fernanda;Santos;19/03/85;22/02/13;E;Inadimplente;21/03/13;1,9;92;Negro;Sorocaba;FIAT;2010",
                line1.toString());

        recordReader.nextKeyValue();
        Text line2 = recordReader.getCurrentValue();

        assertEquals("39;22833930500;Fernando;Parreira;21/03/81;15/03/13;F;Adimplente;25/04/13;1,92;65;Branca;Ribeir\u008Bo Preto;Hyundai;2012",
                line2.toString());

        recordReader.nextKeyValue();
        Text line3 = recordReader.getCurrentValue();

        assertEquals("40;22833930490;Josefina;Ramalho;07/03/90;05/01/13;F;Adimplente;22/04/13;2;100;Branca;Conc\u0097rdia;Hyundai;2013",
                line3.toString());

        // we shouldn't have any more lines to process
        assertEquals(false, recordReader.nextKeyValue());
    }

    @Test
    public void testSetFunction() throws Exception {
        Job job = Job.getInstance();
        CSVRawTextInputFormat.setDelimiter(job, "'");
        CSVRawTextInputFormat.setSeparator(job, ";");
        assertEquals("'", CSVRawTextInputFormat.getDelimiter(job));
        assertEquals(";", CSVRawTextInputFormat.getSeparator(job));
    }

    private Configuration createConfig(String fileName) {
        Configuration conf = new Configuration();

        conf.setStrings("mapred.input.dir", fileName);
        conf.set(CSVFileInputFormat.FORMAT_DELIMITER, "\"");
        conf.set(CSVFileInputFormat.FORMAT_SEPARATOR, ",");

        return conf;
    }
}
