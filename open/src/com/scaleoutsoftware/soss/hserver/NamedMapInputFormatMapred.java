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


import com.scaleoutsoftware.soss.client.CustomSerializer;
import com.scaleoutsoftware.soss.client.map.NamedMap;

import com.scaleoutsoftware.soss.client.map.impl.ChunkBufferPool;
import com.scaleoutsoftware.soss.client.map.impl.ChunkBufferPoolFactory;
import com.scaleoutsoftware.soss.client.map.impl.ConcurrentMapReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.CM_CHUNKSTOREADAHEAD;
import static com.scaleoutsoftware.soss.hserver.HServerParameters.CM_CHUNK_SIZE_KB;
import static com.scaleoutsoftware.soss.hserver.HServerParameters.CM_USEMEMORYMAPPEDFILES;

/**
 * This input format is used to read the input data from a {@link NamedMap}. The mapper will
 * receive keys and values from the map as input keys and values.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class NamedMapInputFormatMapred<K, V> implements InputFormat<K, V> {
    private static final String inputNamedMapKeySerializerProperty = "mapred.hserver.input.namedmapkeyserializer";
    private static final String inputNamedMapValueSerializerProperty = "mapred.hserver.input.namedmapvalueserializer";
    private static final String inputNamedMapKeyProperty = "mapred.hserver.input.namedmapkey";
    private static final String inputNamedMapValueProperty = "mapred.hserver.input.namedmapvalue";
    protected static final String inputAppIdProperty = "mapred.hserver.input.appId";


    /**
     * Sets {@link com.scaleoutsoftware.soss.client.map.NamedMap} as an input source for the job.
     *
     * @param configuration job to modify
     * @param map           name of the map to be used as a job input
     * @param <K>           the type of the key
     * @param <V>           the type of the value
     */
    public static <K, V> void setNamedMap(JobConf configuration, NamedMap<K, V> map) {
        configuration.setInt(inputAppIdProperty, map.getMapId());
        CustomSerializer<K> keySerializer = map.getKeySerializer();
        CustomSerializer<V> valueSerializer = map.getValueSerializer();
        configuration.setClass(inputNamedMapKeySerializerProperty, keySerializer.getClass(), Object.class);
        configuration.setClass(inputNamedMapValueSerializerProperty, valueSerializer.getClass(), Object.class);
        if (keySerializer.getObjectClass() != null) {
            configuration.setClass(inputNamedMapKeyProperty, keySerializer.getObjectClass(), Object.class);
        }
        if (valueSerializer.getObjectClass() != null) {
            configuration.setClass(inputNamedMapValueProperty, valueSerializer.getObjectClass(), Object.class);
        }
    }


    @Override
    public InputSplit[] getSplits(JobConf configuration, int i) throws IOException {
        int mapId = configuration.getInt(inputAppIdProperty, 0);

        if (mapId == 0) {
            throw new IOException("Input format is not configured with a valid NamedMap.");
        }

        List<org.apache.hadoop.mapreduce.InputSplit> splits;

        try {
             splits = GridInputFormat.getSplits(mapId, i);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        InputSplit[] wrappedSpilts = new InputSplit[splits.size()];

        Iterator splitIterator = splits.iterator();

        //Wrap splits to conform to mapred API
        for(i=0; i<wrappedSpilts.length; i++)
        {
            wrappedSpilts[i] =  new BucketSplitMapred((BucketSplit)splitIterator.next());
        }

        return wrappedSpilts;
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf configuration, Reporter reporter) throws IOException {
        int mapId = configuration.getInt(inputAppIdProperty, 0);
        Class<CustomSerializer<K>> keySerializerClass = (Class<CustomSerializer<K>>) configuration.getClass(inputNamedMapKeySerializerProperty, null);
        Class<CustomSerializer<V>> valueSerializerClass = (Class<CustomSerializer<V>>) configuration.getClass(inputNamedMapValueSerializerProperty, null);

        if (mapId == 0 || keySerializerClass == null || valueSerializerClass == null) {
            throw new IOException("Input format is not configured with a valid NamedMap.");
        }

        CustomSerializer<K> keySerializer = ReflectionUtils.newInstance(keySerializerClass, configuration);
        keySerializer.setObjectClass((Class<K>) configuration.getClass(inputNamedMapKeyProperty, null));
        CustomSerializer<V> valueSerializer = ReflectionUtils.newInstance(valueSerializerClass, configuration);
        valueSerializer.setObjectClass((Class<V>) configuration.getClass(inputNamedMapValueProperty, null));
        return new NamedMapRecordReaderMapred(inputSplit, configuration, mapId, keySerializer, valueSerializer);
    }

    /**
     * Record reader for the mapred API.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static class NamedMapRecordReaderMapred<K, V> implements org.apache.hadoop.mapred.RecordReader<K, V> {
        private final ConcurrentMapReader<K, V> reader;
        boolean readDone = false;
        boolean skipAdvance = false;

        public NamedMapRecordReaderMapred(InputSplit inputSplit, Configuration configuration, int mapId, CustomSerializer<K> keySerializer, CustomSerializer<V> valueSerializer) throws IOException {
            int targetChunkSizeKb = HServerParameters.getSetting(CM_CHUNK_SIZE_KB, configuration);
            int chunksToReadAhead = HServerParameters.getSetting(CM_CHUNKSTOREADAHEAD, configuration);
            boolean useMemoryMappedFiles = HServerParameters.getSetting(CM_USEMEMORYMAPPEDFILES, configuration) > 0;

            //Intentionally use NamedMapInputFormat.class as identifier, so mapred and mapreduce can use the same buffer pool
            ChunkBufferPool bufferPool = ChunkBufferPoolFactory.createOrGetBufferPool(NamedMapInputFormat.class, chunksToReadAhead, targetChunkSizeKb*1024, useMemoryMappedFiles);

            reader = new ConcurrentMapReader<K, V>(bufferPool, mapId, keySerializer, valueSerializer);

            if (!(inputSplit instanceof BucketSplitMapred)) {
                throw new IOException("Unexpected split type: " + inputSplit);
            }
            List<WritableBucketId> bucketIds = ((BucketSplitMapred) inputSplit).getBucketIds();
            int[] bucketNumbers = new int[bucketIds.size()];

            for (int i = 0; i < bucketNumbers.length; i++) {
                bucketNumbers[i] = bucketIds.get(i).getBucketNumber() & 0xFFF;
            }
            reader.initialize(bucketNumbers);

            skipAdvance = reader.readNext(); //read one KVP to initialize key and value, if successful, will trigger skip in next()
            readDone = !skipAdvance;
        }

        @Override
        public boolean next(K k, V v) throws IOException {
            if(readDone){
                return false;
            }

            if(skipAdvance)
            {
                skipAdvance = false;
                return true;
            }

            if (!reader.readNext()) {
                reader.close();
                readDone = true;
                return false;
            }

            return true;
        }

        @Override
        public K createKey() {
            return reader.getKey();
        }

        @Override
        public V createValue() {
            return reader.getValue();
        }

        @Override
        public long getPos() throws IOException {
            return readDone ? 1l : 0;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return readDone ? 1.0f : 0.0f;
        }

    }

}
