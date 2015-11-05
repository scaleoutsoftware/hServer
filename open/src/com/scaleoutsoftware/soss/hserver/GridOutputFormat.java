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

import com.scaleoutsoftware.soss.client.*;
import com.scaleoutsoftware.soss.client.map.AvailabilityMode;
import com.scaleoutsoftware.soss.client.map.BulkLoader;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import com.scaleoutsoftware.soss.client.util.SerializationMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.UUID;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.AVAILABILITY_MODE;
import static com.scaleoutsoftware.soss.hserver.HServerParameters.SERIALIZATION_MODE;

/**
 * <p>
 * This output format implementation writes to a named map ({@link NamedMap}) or named cache ({@link NamedCache}).
 * The output map can be set with {@link #setNamedMap(org.apache.hadoop.mapreduce.Job, com.scaleoutsoftware.soss.client.map.NamedMap)}.
 * The output cache name can be set with {@link #setNamedCache(org.apache.hadoop.mapreduce.Job, String)}.
 * </p>
 * <p>
 * If a named cache is used to save the output, the key should be one of the following: {@link Text}, {@link String},
 * {@link CachedObjectId}, {@link UUID} or {@link byte[]}.
 * Values should implement <code>Writable</code> or <code>Serializable</code>. If the values are <code>Writable</code>,
 * a custom serializer ({@link WritableSerializer}) should be set for the <code>NamedCache</code> before
 * accessing the data set through named cache methods.
 * </p>
 * <p>
 * This output format does not preserve the order of key-value pairs. If two values have the same key, only one of them
 * will be saved in the named cache or map.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public class GridOutputFormat<K, V> extends OutputFormat<K, V> {
    private static final String outputNamedCacheProperty = "mapred.hserver.output.namedcache";
    private static final String outputIsNamedMapProperty = "mapred.hserver.output.isnamedmap";
    private static final String outputNamedMapProperty = "mapred.hserver.output.namedmap";
    private static final String outputNamedMapKeySerializerProperty = "mapred.hserver.output.namedmapkeyserializer";
    private static final String outputNamedMapValueSerializerProperty = "mapred.hserver.output.namedmapvalueserializer";
    private static final String outputNamedMapKeyProperty = "mapred.hserver.output.namedmapkey";
    private static final String outputNamedMapValueProperty = "mapred.hserver.output.namedmapvalue";

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        Configuration configuration = taskAttemptContext.getConfiguration();

        if (configuration.getBoolean(outputIsNamedMapProperty, false)) {  //This is a NamedMap
            String mapName = configuration.get(outputNamedMapProperty);
            Class<CustomSerializer<K>> keySerializerClass = (Class<CustomSerializer<K>>) configuration.getClass(outputNamedMapKeySerializerProperty, null);
            Class<CustomSerializer<V>> valueSerializerClass = (Class<CustomSerializer<V>>) configuration.getClass(outputNamedMapValueSerializerProperty, null);
            int smOrdinal = configuration.getInt(SERIALIZATION_MODE, SerializationMode.DEFAULT.ordinal());
            int amOrdinal = configuration.getInt(AVAILABILITY_MODE, AvailabilityMode.USE_REPLICAS.ordinal());
            SerializationMode serializationMode = SerializationMode.values()[smOrdinal];
            AvailabilityMode availabilityMode = AvailabilityMode.values()[amOrdinal];

            if (mapName == null || mapName.length() == 0 || keySerializerClass == null || valueSerializerClass == null) {
                throw new IOException("Input format is not configured with a valid NamedMap.");
            }

            CustomSerializer<K> keySerializer = ReflectionUtils.newInstance(keySerializerClass, configuration);
            keySerializer.setObjectClass((Class<K>) configuration.getClass(outputNamedMapKeyProperty, null));
            CustomSerializer<V> valueSerializer = ReflectionUtils.newInstance(valueSerializerClass, configuration);
            valueSerializer.setObjectClass((Class<V>) configuration.getClass(outputNamedMapValueProperty, null));
            NamedMap<K, V> namedMap = NamedMapFactory.getMap(mapName, keySerializer, valueSerializer);
            namedMap.setSerializationMode(serializationMode);
            namedMap.setAvailabilityMode(availabilityMode);
            return new NamedMapRecordWriter<K, V>(namedMap);
        } else {  //This is a NamedCache
            String cacheName = configuration.get(outputNamedCacheProperty);
            if (cacheName == null || cacheName.length() == 0) throw new IOException("Output NamedCache not specified.");

            NamedCache cache;

            try {
                cache = CacheFactory.getCache(cacheName);
            } catch (NamedCacheException e) {
                throw new IOException("Cannot initialize NamedCache.", e);
            }

            Class valueClass = taskAttemptContext.getOutputValueClass();
            if (Writable.class.isAssignableFrom(valueClass)) {
                cache.setCustomSerialization(new WritableSerializer(valueClass));
            }

            return new NamedCacheRecordWriter<K, V>(cache);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //Do nothing.
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new GridOutputCommitter();
    }

    /**
     * Sets the name of the output {@link NamedCache}.
     *
     * @param job       job to modify
     * @param cacheName name of the output cache.
     */
    public static void setNamedCache(Job job, String cacheName) {
        job.getConfiguration().setStrings(outputNamedCacheProperty, cacheName);
    }

    /**
     * Sets the {@link NamedMap} to direct output to.
     *
     * @param job job to modify
     * @param map named map to be used for output
     */
    public static void setNamedMap(Job job, NamedMap map) {
        Configuration configuration = job.getConfiguration();
        configuration.setBoolean(outputIsNamedMapProperty, true);
        configuration.setStrings(outputNamedMapProperty, map.getMapName());
        CustomSerializer keySerializer = map.getKeySerializer();
        CustomSerializer valueSerializer = map.getValueSerializer();
        SerializationMode serializationMode = map.getSerializationMode();
        AvailabilityMode availabilityMode = map.getAvailabilityMode();
        configuration.setInt(SERIALIZATION_MODE, serializationMode.ordinal());
        configuration.setInt(AVAILABILITY_MODE, availabilityMode.ordinal());
        configuration.setClass(outputNamedMapKeySerializerProperty, keySerializer.getClass(), Object.class);
        configuration.setClass(outputNamedMapValueSerializerProperty, valueSerializer.getClass(), Object.class);
        if (keySerializer.getObjectClass() != null) {
            configuration.setClass(outputNamedMapKeyProperty, keySerializer.getObjectClass(), Object.class);
        }
        if (valueSerializer.getObjectClass() != null) {
            configuration.setClass(outputNamedMapValueProperty, valueSerializer.getObjectClass(), Object.class);
        }
    }

    /**
     * This record writer outputs values to the named map.
     *
     * @param <K> key type
     * @param <V> value type
     */
    class NamedMapRecordWriter<K, V> extends RecordWriter<K, V> {
        BulkLoader<K, V> bulkLoader;

        NamedMapRecordWriter(NamedMap<K, V> map) {
            bulkLoader = map.getBulkLoader();
        }

        @Override
        public void write(final K k, final V v) throws IOException, InterruptedException {
            bulkLoader.put(k, v);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            bulkLoader.close();
        }
    }

    /**
     * This record writer outputs values to the named cache, converting keys to named cache keys.
     *
     * @param <K> key type
     * @param <V> value type
     */
    class NamedCacheRecordWriter<K, V> extends RecordWriter<K, V> {
        final NamedCache cache;


        NamedCacheRecordWriter(NamedCache cache) {
            this.cache = cache;
        }


        @Override
        public void write(final K k, final V v) throws IOException, InterruptedException {
            try {
                cache.put(getKey(k), v);
            } catch (NamedCacheException e) {
                throw new IOException("ScaleOut StateServer threw an error", e);
            }
        }

        private CachedObjectId getKey(Object key) throws NamedCacheException, IOException {
            if (key instanceof Text || key instanceof String) {
                return cache.createKey(key.toString());
            } else if (key instanceof CachedObjectId) {
                return ((CachedObjectId) key);
            } else if (key instanceof byte[]) {
                return cache.createKey((byte[]) key);
            } else if (key instanceof UUID) {
                return cache.createKey((UUID) key);
            } else {
                throw new IOException("Illegal key type " + key.getClass().getName());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            cache.setCustomSerialization(null);
        }

    }

    /**
     * A stub output committer, no special actions required on its part.
     */
    class GridOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
            //Do nothing.
        }

        @Override
        public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
            //Do nothing.
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
            //Grid output tasks do not need commit
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
            //Do nothing, this method should never be called.
        }

        @Override
        public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
            //We do not have a specific actions for aborting a task.
        }

        public void cleanupJob(JobContext context) {
        }
    }
}
