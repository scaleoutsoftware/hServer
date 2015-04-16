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
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import com.scaleoutsoftware.soss.hserver.interop.BucketStoreFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;


/**
 * This record reader is used for splits that have already been recorded (meaning that the corresponding output of the
 * mappers is already saved to the StateServer). This record reader prefetches several key-value pairs, so they are readily
 * available for the mapper.
 *
 * @param <K> key type
 * @param <V> value type
 * @see CachingRecordReader
 */
class ReplayingRecordReader<K extends Writable, V extends Writable> extends RecordReader<K, V> {

    private ImageInputSplit inputSplit;
    private BucketReader<K, V> bucketReader;
    private int numberProcessed;


    @Override
    @SuppressWarnings("unchecked")
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        inputSplit = (ImageInputSplit) split;
        BucketStore bucketStore;
        KeyValuePair<K, V> keyValuePair;
        try {
            bucketStore = BucketStoreFactory.getBucketStore(inputSplit.getImageIdString());
        } catch (StateServerException e) {
            throw new IOException("Cannot access ScaleOut StateServer", e);
        }
        if (inputSplit.isRecorded()) {
            Configuration conf = context.getConfiguration();

            try {
                keyValuePair = DatasetInputFormat.createFixedKeyValueSizePair(context.getConfiguration());
                if (keyValuePair == null) {  //No fixed size KVP specified
                    Class keyClass = Class.forName(inputSplit.getKeyClass());
                    Class valueClass = Class.forName(inputSplit.getValueClass());
                    keyValuePair = new KeyValuePair<K, V>((K) ReflectionUtils.newInstance(keyClass, conf), (V) ReflectionUtils.newInstance(valueClass, conf));
                }
            } catch (Exception e) {
                throw new IOException("Cannot find key or value class.", e);
            }
            bucketReader = new BucketReader<K, V>(bucketStore, inputSplit.getBucketId(), inputSplit.getNumberOfChunks(), context.getConfiguration(), keyValuePair);
            bucketReader.startReading();

            numberProcessed = 0;
        } else {
            throw new IOException("Cannot replay split, it was not recorded");
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (bucketReader == null) {
            throw new IOException("The reader was not initialized");
        }
        boolean hasNext;
        try {
            hasNext = bucketReader.readNext();
        } catch (Exception e) {
            throw new IOException("Cannot access ScaleOut StateServer", e);
        }

        if (hasNext) {
            numberProcessed++;
            return true;
        } else {
            if (numberProcessed != inputSplit.getNumberOfKeys())
                throw new IOException("Recorded split is corrupted, number of keys does not match:" + numberProcessed + "," + inputSplit.getNumberOfKeys());
            return false;
        }

    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return bucketReader.getKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return bucketReader.getValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) numberProcessed / (float) inputSplit.getNumberOfKeys();
    }

    @Override
    public void close() throws IOException {
    }

}
