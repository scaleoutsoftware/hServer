/*
 Copyright (c) 2015 by ScaleOut Software, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.scaleoutsoftware.soss.hserver;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 * This record reader is the point-of-entry record reader. The actual requests are forwarded to one of
 * two record readers, depending on whether this is recording or replaying stage.
 *
 * @param <K> key type
 * @param <V> value type
 * @see CachingRecordReader
 * @see ReplayingRecordReader
 */
class DatasetRecordReader<K extends Writable, V extends Writable> extends RecordReader<K, V> {
    private static final Log LOG = LogFactory.getLog(DatasetRecordReader.class);

    private CachingRecordReader<K, V> cachingRecordReader;    //Record reader for the recording phase
    private ReplayingRecordReader<K, V> replayingRecordReader; //Record reader for the replaying phase
    private RecordReader<K, V> currentRecordReader; //Currently selected record reader

    //Timer variables for collecting statistics

    private long totalTime = 0;
    private long numberOfRecors = 0;
    private long maxTime = 0;
    private long minTime = 100000000L;
    private long initialize = 0;
    private long first = 0;

    public DatasetRecordReader(RecordReader<K, V> fallBackRecordReader) {
        cachingRecordReader = new CachingRecordReader<K, V>(fallBackRecordReader);
        replayingRecordReader = new ReplayingRecordReader<K, V>();
    }


    /**
     * This method chooses current record reader (caching or replaying) based on the information in the split.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        numberOfRecors = 0;
        if (!(inputSplit instanceof ImageInputSplit)) {
            throw new IOException("Unexpected split type: " + inputSplit);
        }
        if (!((ImageInputSplit) inputSplit).isRecorded()) {
            currentRecordReader = cachingRecordReader;
        } else {
            currentRecordReader = replayingRecordReader;
        }

        initialize = System.nanoTime();
        currentRecordReader.initialize(inputSplit, taskAttemptContext);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        long time;
        initialize = System.nanoTime() - initialize;
        time = System.nanoTime();
        boolean hasNext = currentRecordReader.nextKeyValue();
        time = System.nanoTime() - time;

        if (first == 0) first = time;
        maxTime = Math.max(maxTime, time);
        minTime = Math.min(minTime, time);
        totalTime += time;

        if (hasNext) {
            numberOfRecors++;
        }

        if (!hasNext && numberOfRecors > 1) {
            LOG.debug("Record reader done. #rec = " + numberOfRecors + "; Total=" + totalTime + " ns; Per_rec = " + (totalTime / numberOfRecors) + " ns. First = " + first + "; Max = " + maxTime + "; Min" + minTime + " Initial = " + initialize);
        }

        return hasNext;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return currentRecordReader.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return currentRecordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return currentRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        currentRecordReader.close();
    }
}
