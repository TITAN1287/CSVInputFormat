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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Configurable CSV line reader. Variant of TextInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column
 * 
 *
 * @author mvallebr, tristeng
 *
 * October, 2015: tristeng (tgeorgiou@phemi.com) added in the getSplits() functionality to ensure we don't split in the
 *                middle of CSV row, which would be really, really bad. This would likely happen when the input CSV file
 *                is larger than block size * SPLIT_SLOP. Pushed split functionality into a parent class.
 *
 */
public class CSVTextInputFormat extends CSVFileInputFormat<LongWritable, List<Text>> {

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordReader<LongWritable, List<Text>> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        String delimiter = conf.get(FORMAT_DELIMITER);
        String separator = conf.get(FORMAT_SEPARATOR);
        if (null == delimiter || null == separator) {
            throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
        }
        if (delimiter.length() != 1 || separator.length() != 1) {
            throw new IOException("CSVTextInputFormat: delimiter/separator can only be a single character");
        }
        if (delimiter.equals(separator)) {
            throw new IOException("CSVTextInputFormat: delimiter and separator cannot be the same character");
        }
        return new CSVLineRecordReader();
    }
}