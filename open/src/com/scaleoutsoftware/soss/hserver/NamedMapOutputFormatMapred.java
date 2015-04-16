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
import com.scaleoutsoftware.soss.client.map.BulkLoader;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * <p>
 * This output format implementation writes to a named map ({@link NamedMap}).
 * The output map can be set with {@link #setNamedMap(org.apache.hadoop.mapred.JobConf, com.scaleoutsoftware.soss.client.map.NamedMap)}.
 * </p>
 * This output format does not preserve the order of key-value pairs. If two values have the same key, only one of them
 * will be saved in the named cache or map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class NamedMapOutputFormatMapred<K,V> implements OutputFormat<K,V> {
    private static final String outputNamedMapProperty = "mapred.hserver.output.namedmap";
    private static final String outputNamedMapKeySerializerProperty = "mapred.hserver.output.namedmapkeyserializer";
    private static final String outputNamedMapValueSerializerProperty = "mapred.hserver.output.namedmapvalueserializer";
    private static final String outputNamedMapKeyProperty = "mapred.hserver.output.namedmapkey";
    private static final String outputNamedMapValueProperty = "mapred.hserver.output.namedmapvalue";

    @Override
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf configuration, String s, Progressable progressable) throws IOException {
        String mapName = configuration.get(outputNamedMapProperty);
        Class<CustomSerializer<K>> keySerializerClass = (Class<CustomSerializer<K>>) configuration.getClass(outputNamedMapKeySerializerProperty, null);
        Class<CustomSerializer<V>> valueSerializerClass = (Class<CustomSerializer<V>>) configuration.getClass(outputNamedMapValueSerializerProperty, null);

        if (mapName == null || mapName.length() == 0 || keySerializerClass == null || valueSerializerClass == null) {
            throw new IOException("Input format is not configured with a valid NamedMap.");
        }

        CustomSerializer<K> keySerializer = ReflectionUtils.newInstance(keySerializerClass, configuration);
        keySerializer.setObjectClass((Class<K>) configuration.getClass(outputNamedMapKeyProperty, null));
        CustomSerializer<V> valueSerializer = ReflectionUtils.newInstance(valueSerializerClass, configuration);
        valueSerializer.setObjectClass((Class<V>) configuration.getClass(outputNamedMapValueProperty, null));
        NamedMap<K, V> namedMap = NamedMapFactory.getMap(mapName, keySerializer, valueSerializer);
        return new NamedMapRecordWriter<K, V>(namedMap);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf configuration) throws IOException {
        String mapName = configuration.get(outputNamedMapProperty);
        Class<CustomSerializer<K>> keySerializerClass = (Class<CustomSerializer<K>>) configuration.getClass(outputNamedMapKeySerializerProperty, null);
        Class<CustomSerializer<V>> valueSerializerClass = (Class<CustomSerializer<V>>) configuration.getClass(outputNamedMapValueSerializerProperty, null);

        if (mapName == null || mapName.length() == 0 || keySerializerClass == null || valueSerializerClass == null) {
            throw new IOException("Input format is not configured with a valid NamedMap.");
        }
    }

    /**
     * Sets the {@link NamedMap} to direct output to.
     *
     * @param configuration job to modify
     * @param map named map to be used for output
     */
    public static void setNamedMap(JobConf configuration, NamedMap map) {
        configuration.setStrings(outputNamedMapProperty, map.getMapName());
        CustomSerializer keySerializer = map.getKeySerializer();
        CustomSerializer valueSerializer = map.getValueSerializer();
        configuration.setClass(outputNamedMapKeySerializerProperty, keySerializer.getClass(), Object.class);
        configuration.setClass(outputNamedMapValueSerializerProperty, valueSerializer.getClass(), Object.class);
        if (keySerializer.getObjectClass() != null) {
            configuration.setClass(outputNamedMapKeyProperty, keySerializer.getObjectClass(), Object.class);
        }
        if (valueSerializer.getObjectClass() != null) {
            configuration.setClass(outputNamedMapValueProperty, valueSerializer.getObjectClass(), Object.class);
        }
    }

    static class NamedMapRecordWriter<K,V> implements RecordWriter<K,V>
    {
        private BulkLoader<K, V> bulkLoader;

        NamedMapRecordWriter(NamedMap<K, V> map) {
            bulkLoader = map.getBulkLoader();
        }

        @Override
        public void write(K k, V v) throws IOException {
            bulkLoader.put(k, v);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            bulkLoader.close();
        }
    }
}
