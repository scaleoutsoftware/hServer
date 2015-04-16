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


import com.scaleoutsoftware.soss.client.CachedObjectId;
import com.scaleoutsoftware.soss.client.NamedCache;
import com.scaleoutsoftware.soss.client.NamedCacheException;
import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.client.da.StateServerKey;
import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * This input format implementation reads the data from a {@link NamedCache}. It assumes that all the objects in the
 * cache have the same type. The key that the mapper will receive is of type {@link com.scaleoutsoftware.soss.client.CachedObjectId}.
 * To set up this input format, set the input cache and the type of objects stored in it by calling
 * {@link #setNamedCache(org.apache.hadoop.mapreduce.Job, com.scaleoutsoftware.soss.client.NamedCache, Class)}.
 * </p>
 * <p>
 * The desired number of input splits can be optionally set by
 * {@link #setSuggestedNumberOfSplits(org.apache.hadoop.mapreduce.Job, int)}.
 * </p>
 *
 * @param <V> type of the value
 */
public class NamedCacheInputFormat<V> extends GridInputFormat<CachedObjectId<V>, V> {

    private static final String inputObjectClassProperty = "mapred.hserver.input.objectclass";


    private static final int PREFETCHING_DEPTH = 5;
    private static final int INITIAL_BUFFER_SIZE = 100;


    /**
     * Sets the name of the input named cache and the type of the objects in the named cache.
     *
     * @param job         job to modify
     * @param cache       input named cache
     * @param objectClass type of the objects in the named cache
     * @throws NamedCacheException if a ScaleOut hServer access error occurred
     */
    public static void setNamedCache(Job job, NamedCache cache, Class objectClass) throws NamedCacheException {
        job.getConfiguration().setInt(inputAppIdProperty, cache.createKey("temp").getKey().getAppId());
        job.getConfiguration().setClass(inputObjectClassProperty, objectClass, Object.class);
    }


    /**
     * This record reader reads a list of {@link com.scaleoutsoftware.soss.client.da.StateServerKey} from the StateServer. It uses
     * {@link ObjectPrefetcher} to do asynchronous reads.
     *
     * @param <V> type of the object
     */
    class BucketRecordReader<V> extends RecordReader<CachedObjectId<V>, V> {

        ObjectPrefetcher<V> prefetcher;
        long numberRead = 0;
        long numberTotal;
        Class<V> objectClass;

        BucketRecordReader(Class<V> objectClass) {
            this.objectClass = objectClass;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (!(inputSplit instanceof BucketSplit)) {
                throw new IOException("Unexpected split type: " + inputSplit);
            }
            List<StateServerKey> keyList = new ArrayList<StateServerKey>();
            try {
                for (BucketId id : ((BucketSplit) inputSplit).getBucketIds()) {
                    keyList.addAll(BucketStore.getNamedCacheBucketContents(id));
                }
            } catch (StateServerException e) {
                throw new IOException("Cannot access ScaleOut StateServer.", e);
            }
            numberTotal = keyList.size();
            prefetcher = new ObjectPrefetcher<V>(keyList, PREFETCHING_DEPTH, objectClass, null, taskAttemptContext.getConfiguration(), INITIAL_BUFFER_SIZE);
            prefetcher.startPrefetching();

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            try {
                return prefetcher.readNext();
            } catch (Exception e) {
                throw new IOException("Error while reading object.", e);
            }
        }

        @Override
        public CachedObjectId<V> getCurrentKey() throws IOException, InterruptedException {
            return prefetcher.nextKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return prefetcher.nextObject();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) numberRead / (float) numberTotal;
        }

        @Override
        public void close() throws IOException {

        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public BucketRecordReader<V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Class objectClass = taskAttemptContext.getConfiguration().getClass(inputObjectClassProperty, null);
        if (objectClass == null) {
            throw new IOException("Type of the input objects in the NamedCache cache is not set.");
        }
        return new BucketRecordReader<V>(objectClass);
    }
}
