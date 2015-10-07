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

import com.google.common.base.Stopwatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * File input format that can create splits on CSV files
 *
 * @author tristeng (tgeorgiou@phemi.com)
 *
 * @param <K> the key to emit
 * @param <V> the value to emit
 */
public abstract class CSVFileInputFormat<K, V> extends FileInputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(CSVTextInputFormat.class);
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    public static final String FORMAT_DELIMITER = "mapreduce.csvinput.delimiter";
    public static final String FORMAT_SEPARATOR = "mapreduce.csvinput.separator";
    public static final String DEFAULT_DELIMITER = "\"";
    public static final String DEFAULT_SEPARATOR = ",";

    /**
     * Very similar to the FileInputFormat implementation but makes sure to split on a CSV row boundary. Since we have
     * to parse the entire file, expect this function to be slow for huge files
     *
     * @param job the job context
     * @return the file splits
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        Stopwatch sw = new Stopwatch().start();
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        Text row = new Text();
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
                    // NOTE: using the CSVRawLineRecordReader instead of CSVLineRecordReader saw performance
                    // gains of ~25%
                    CSVRawLineRecordReader reader = new CSVRawLineRecordReader(
                            new FSDataInputStream(fs.open(file.getPath())),
                            job.getConfiguration());
                    long blockSize = file.getBlockSize();
                    long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                    long bytesRemaining = length, startPos = 0;
                    while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                        long bytesRead = 0, bytesInLine;
                        // read CSV lines until we are at least split size large
                        while (bytesRead < splitSize) {
                            bytesInLine = reader.readLine(row);
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
                    reader.close();
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

    public static void setDelimiter(Job job, String delimiter) {
        job.getConfiguration().set(FORMAT_DELIMITER, delimiter);
    }

    public static void setSeparator(Job job, String separator) {
        job.getConfiguration().set(FORMAT_SEPARATOR, separator);
    }

    public static String getDelimiter(Job job) {
        return job.getConfiguration().get(FORMAT_DELIMITER, DEFAULT_DELIMITER);
    }

    public static String getSeparator(Job job) {
        return job.getConfiguration().get(FORMAT_SEPARATOR, DEFAULT_SEPARATOR);
    }
}
