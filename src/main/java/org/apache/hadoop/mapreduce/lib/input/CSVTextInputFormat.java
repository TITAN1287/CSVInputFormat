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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Stopwatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Configurable CSV line reader. Variant of TextInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column
 * 
 *
 * @author mvallebr, tristeng
 *
 * October, 2015: tristeng (tgeorgiou@phemi.com) added in the getSplits() functionality to ensure we don't split in the
 *                middle of CSV row, which would be really, really bad. This would likely happen when the input CSV file
 *                is larger than block size * SPLIT_SLOP
 *
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>> {
    private static final Log LOG = LogFactory.getLog(CSVTextInputFormat.class);
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */

    @Override
    public RecordReader<LongWritable, List<Text>> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        String quote = conf.get(CSVLineRecordReader.FORMAT_DELIMITER);
        String separator = conf.get(CSVLineRecordReader.FORMAT_SEPARATOR);
        if (null == quote || null == separator) {
            throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
        }
        return new CSVLineRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Stopwatch sw = new Stopwatch().start();
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        List<Text> cells = new ArrayList<Text>();
        for (FileStatus file: files) {
            Path path = file.getPath();
            long length = file.getLen();
            if (length != 0) {
                BlockLocation[] blkLocations;
                FileSystem fs = path.getFileSystem(job.getConfiguration());
                if (file instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                    blkLocations = fs.getFileBlockLocations(file, 0, length);
                }
                if (isSplitable(job, path)) {
                    CSVLineRecordReader csvLineRecordReader = new CSVLineRecordReader(
                            new FSDataInputStream(fs.open(file.getPath())),
                            job.getConfiguration());
                    long blockSize = file.getBlockSize();
                    long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                    long bytesRemaining = length, startPos = 0;
                    while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                        long bytesRead = 0, bytesInLine;
                        // read CSV lines until we are at least split size large
                        while (bytesRead < splitSize) {
                            bytesInLine = csvLineRecordReader.readLine(cells);
                            // if we read 0 bytes, we have hit EOF
                            if (bytesInLine <= 0) {
                                // NOTE: I don't think we should ever reach EOF actually; because of SPLIT_SLOP
                                //       it won't break anything and I would rather program defensively
                                LOG.debug("Reached EOF while splitting; this is unexpected");
                                break;
                            }
                            bytesRead += bytesInLine;
                        }
                        // if we read 0 bytes, we have hit EOF
                        if (bytesRead <= 0) {
                            // NOTE: I don't think we should ever reach EOF actually; because of SPLIT_SLOP
                            //       it won't break anything and I would rather program defensively
                            LOG.debug("Reached EOF while splitting; this is unexpected");
                            break;
                        }
                        int blkIndex = getBlockIndex(blkLocations, startPos);
                        splits.add(makeSplit(path, startPos, bytesRead,
                                blkLocations[blkIndex].getHosts(),
                                blkLocations[blkIndex].getCachedHosts()));
                        // increment start position by the number of bytes we have read
                        startPos += bytesRead;
                        bytesRemaining -= bytesRead;
                    }
                    if (bytesRemaining != 0) {
                        int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                        splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                                blkLocations[blkIndex].getHosts(),
                                blkLocations[blkIndex].getCachedHosts()));
                    }
                } else { // not splitable
                    splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
                            blkLocations[0].getCachedHosts()));
                }
            } else {
                // create empty hosts array for zero length files
                splits.add(makeSplit(path, 0, length, new String[0]));
            }
        }

        // Save the number of input files for metrics/loadgen
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
        sw.stop();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Total # of splits generated by getSplits: " + splits.size()
                    + ", TimeTaken: " + sw.elapsed(MILLISECONDS));
        }

        return splits;
    }
}