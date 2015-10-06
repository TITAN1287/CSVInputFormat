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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * CSVRawTextInputFormat reads a valid CSV file and outputs each CSV row in it's raw form
 *
 * @author tristeng (tgeorgiou@phemi.com)
 *
 */
public class CSVRawTextInputFormat extends CSVFileInputFormat<LongWritable, Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String delimiter = conf.get(FORMAT_DELIMITER);
        if (null == delimiter) {
            throw new IOException("CSVRawTextInputFormat: missing parameter delimiter");
        }
        if (delimiter.length() != 1) {
            throw new IOException("CSVRawTextInputFormat: delimiter can only be a single character");
        }
        return new CSVRawLineRecordReader();
    }
}
