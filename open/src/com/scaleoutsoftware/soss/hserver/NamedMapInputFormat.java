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
import com.scaleoutsoftware.soss.client.map.AvailabilityMode;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.util.SerializationMode;
import com.scaleoutsoftware.soss.client.map.impl.ChunkBufferPool;
import com.scaleoutsoftware.soss.client.map.impl.ChunkBufferPoolFactory;
import com.scaleoutsoftware.soss.client.map.impl.ConcurrentMapReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.List;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.*;

/**
 * This input format is used to read the input data from a {@link NamedMap}. The mapper will
 * receive keys and values from the map as input keys and values.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class NamedMapInputFormat<K, V> extends GridInputFormat<K, V> {
    private static final String inputNamedMapKeySerializerProperty = "mapred.hserver.input.namedmapkeyserializer";
    private static final String inputNamedMapValueSerializerProperty = "mapred.hserver.input.namedmapvalueserializer";
    private static final String inputNamedMapKeyProperty = "mapred.hserver.input.namedmapkey";
    private static final String inputNamedMapValueProperty = "mapred.hserver.input.namedmapvalue";


    /**
     * Sets {@link com.scaleoutsoftware.soss.client.map.NamedMap} as an input source for the job.
     *
     * @param job job to modify
     * @param map name of the map to be used as a job input
     * @param <K> the type of the key
     * @param <V> the type of the value
     */
    public static <K, V> void setNamedMap(Job job, NamedMap<K, V> map) {
        Configuration configuration = job.getConfiguration();
        configuration.setInt(inputAppIdProperty, map.getMapId());
        CustomSerializer<K> keySerializer = map.getKeySerializer();
        CustomSerializer<V> valueSerializer = map.getValueSerializer();
        SerializationMode serializationMode = map.getSerializationMode();
        AvailabilityMode availabilityMode = map.getAvailabilityMode();
        configuration.setInt(SERIALIZATION_MODE, serializationMode.ordinal());
        configuration.setInt(AVAILABILITY_MODE, availabilityMode.ordinal());
        configuration.setClass(inputNamedMapKeySerializerProperty, keySerializer.getClass(), Object.class);
        configuration.setClass(inputNamedMapValueSerializerProperty, valueSerializer.getClass(), Object.class);
        if (keySerializer.getObjectClass() != null) {
            configuration.setClass(inputNamedMapKeyProperty, keySerializer.getObjectClass(), Object.class);
        }
        if (valueSerializer.getObjectClass() != null) {
            configuration.setClass(inputNamedMapValueProperty, valueSerializer.getObjectClass(), Object.class);
        }

    }


    class NamedMapReader<K, V> extends RecordReader<K, V> {
        private final ConcurrentMapReader<K, V> reader;


        NamedMapReader(Configuration configuration, int mapId, CustomSerializer<K> keySerializer, CustomSerializer<V> valueSerializer, SerializationMode serializationMode) throws IOException {
            int targetChunkSizeKb = HServerParameters.getSetting(CM_CHUNK_SIZE_KB, configuration);
            int chunksToReadAhead = HServerParameters.getSetting(CM_CHUNKSTOREADAHEAD, configuration);
            boolean useMemoryMappedFiles = HServerParameters.getSetting(CM_USEMEMORYMAPPEDFILES, configuration) > 0;
            ChunkBufferPool bufferPool = ChunkBufferPoolFactory.createOrGetBufferPool(NamedMapInputFormat.class, chunksToReadAhead, targetChunkSizeKb*1024, useMemoryMappedFiles);
            reader = new ConcurrentMapReader<K, V>(bufferPool, mapId, keySerializer, valueSerializer, serializationMode);
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (!(inputSplit instanceof BucketSplit)) {
                throw new IOException("Unexpected split type: " + inputSplit);
            }
            List<WritableBucketId> bucketIds = ((BucketSplit) inputSplit).getBucketIds();
            int[] bucketNumbers = new int[bucketIds.size()];

            for (int i = 0; i < bucketNumbers.length; i++) {
                bucketNumbers[i] = bucketIds.get(i).getBucketNumber() & 0xFFF;
            }
            reader.initialize(bucketNumbers);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!reader.readNext()) {
                reader.close();
                return false;
            }
            return true;
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return reader.getKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return reader.getValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration configuration = taskAttemptContext.getConfiguration();
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
        int smOrdinal = configuration.getInt(SERIALIZATION_MODE, SerializationMode.DEFAULT.ordinal());
        SerializationMode serializationMode = SerializationMode.values()[smOrdinal];
        return new NamedMapReader<K, V>(configuration, mapId, keySerializer, valueSerializer, serializationMode);
    }
}
