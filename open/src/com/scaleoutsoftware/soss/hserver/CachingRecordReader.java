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


import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import com.scaleoutsoftware.soss.hserver.interop.BucketStoreFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This record reader intercepts the key-value pairs from the underlying input format
 * and saves them in the StateServer. It uses {@link BucketWriter} to perform asynchronous writes to
 * the named cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
class CachingRecordReader<K extends Writable, V extends Writable> extends RecordReader<K, V> {
    private static final Log LOG = LogFactory.getLog(CachingRecordReader.class);

    private ImageInputSplit split;
    private BucketId BucketId;
    private int numberOfKeys = 0;
    private int numberOfChunks = 0;
    private RecordReader<K, V> fallBackRecordReader;
    private KeyValuePair<K, V> keyValuePair;
    private BucketStore bucketStore;
    private BucketWriter<K, V> bucketWriter;

    private boolean sossAvailable;
    private TaskAttemptContext context;


    CachingRecordReader(RecordReader<K, V> fallBackRecordReader) {
        this.fallBackRecordReader = fallBackRecordReader;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (ImageInputSplit) split;
        this.context = context;
        LOG.info("Recording split: " + split);
        fallBackRecordReader.initialize(((ImageInputSplit) split).getFallbackInputSplit(), context);

        try {
            bucketStore = BucketStoreFactory.getBucketStore(this.split.getImageIdString());
            BucketId = ((ImageInputSplit) split).getBucketId();

            sossAvailable = true;

            if (BucketId.isDummyId()) //This split has never been recorded
            {
                BucketId = bucketStore.getNextLocalBucketId();
            }
            LOG.debug("Updating image before recording. " + this.split.getImageIdString());
            updateImageBeforeRecording(context.getTaskAttemptID().getTaskID().toString());
        } catch (StateServerException e) {
            LOG.error("Cannot connect to ScaleOut StateServer.", e);
            sossAvailable = false;
        }
        numberOfKeys = 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (fallBackRecordReader.nextKeyValue()) {
            if (bucketWriter == null && sossAvailable) {
                keyValuePair = DatasetInputFormat.createFixedKeyValueSizePair(context.getConfiguration());
                if (keyValuePair == null) {
                    keyValuePair = new KeyValuePair<K, V>(fallBackRecordReader.getCurrentKey(), fallBackRecordReader.getCurrentValue());
                }

                try {
                    bucketWriter = new BucketWriter<K, V>(bucketStore, BucketId, context.getConfiguration(), keyValuePair);
                } catch (StateServerException e) {
                    LOG.error("Cannot connect to ScaleOut StateServer.", e);
                    sossAvailable = false;
                }
            }

            if (sossAvailable) {
                try {
                    bucketWriter.put(fallBackRecordReader.getCurrentKey(), fallBackRecordReader.getCurrentValue());
                    numberOfKeys++;
                } catch (IOException e) {
                    LOG.error("Cannot connect to ScaleOut StateServer.", e);
                    sossAvailable = false;
                }
            }
            return true;
        } else {
            if (sossAvailable) {
                bucketWriter.close();
                numberOfChunks = bucketWriter.getNumberOfChunks();
                updateImageAfterRecording();
            }
            return false;
        }
    }


    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return fallBackRecordReader.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return fallBackRecordReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return fallBackRecordReader.getProgress();
    }

    /**
     * Performs the initial check in of the split, to let other tasks
     * know that this split is already being recorded.
     *
     * @param taskId current task id
     */
    private void updateImageBeforeRecording(String taskId) throws IOException, StateServerException {
        try {
            if (!sossAvailable) return;
            split.setBucketId(BucketId);
            if (!GridImage.inputSplitCheckInBeforeRecording(split, taskId)) {
                throw new IOException("Split is already being processed by other task attempt, exiting.");
            }
            LOG.debug("Split checked in. " + split);
        } catch (ClassNotFoundException e) {
            throw new IOException("Error while checking split in", e);
        }

    }

    /**
     * Performs the final check in of the split after recording is complete.
     */
    private void updateImageAfterRecording() throws IOException {
        if (!sossAvailable) return;
        split.setNumberOfKeys(numberOfKeys);
        split.setNumberOfChunks(numberOfChunks);
        split.setRecorded(true);
        if (keyValuePair.getKey() != null && keyValuePair.getValue() != null) {
            split.setKeyValueClass(keyValuePair.getKey().getClass(), keyValuePair.getValue().getClass());
        }
        try {
            GridImage.inputSplitCheckInAfterRecording(split);
            LOG.debug("Split checked in. " + split);
        } catch (Exception e) {
            LOG.error("Exception occurred while checking split in, check in aborted", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            //To be safe, make sure we are not holding a lock.
            bucketStore.unlockImage();
        } catch (Exception e) {
            //Do nothing
        }
        fallBackRecordReader.close();
    }
}
