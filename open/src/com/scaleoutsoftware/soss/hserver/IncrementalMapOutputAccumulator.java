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

import com.scaleoutsoftware.soss.hserver.interop.DataGridChunkedCollectionWriter;
import com.scaleoutsoftware.soss.hserver.interop.DataGridWriterParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This accumulator uses incremental combining to compact the output. It maintains an inner map of key-value pairs. If the key of the incoming key-value
 * pair is already in the map, then {@link org.apache.hadoop.mapreduce.Reducer#reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)}
 * is called for the incoming value and existing value. It is expected that in most cases the combiner will emit one value with the same key,
 * so the binary combining is made. If the key does not match, or there is more than one value, the non-matching output of the combiner is forwarded to the
 * beginning of the pipeline.
 * <p/>
 * There are several important optimisations contained in that class:
 * <p/>
 * {@link org.apache.hadoop.util.ReflectionUtils#clone()} does two things: creates a new instance through reflection and then copies the contents of the object to that instance through
 * serialization/deserealization pipeline. Of that two steps, creating new instance is the heaviest. We make sure to clone only the first occurrence of the
 * value for the key, and then copy subsequent values into this value.
 * <p/>
 * There are custom copying methods for a few Writables that are used most often, which help to substitute general case serialization/deserialization with
 * simple field copying, if possible.
 * <p/>
 * When the combiner gets the {@link Iterable} collection of values, the first value is always the one already entered in the hash map. IF the combiner is
 * implemented in such a way that it puts the result in that value, there is no need for copying at all. This class detects that case.
 *
 * @param <K> key type
 * @param <V> value type
 */
class IncrementalMapOutputAccumulator<K, V> extends WrappingMapOutputAccumulator<K, V> {
    private Map<K, V> keyValueAccumulator = new HashMap<K, V>(300000);
    private boolean keyIsWritable, valueIsWritable;
    private K currentlyCombiningKey;
    private V currentlyCombiningValue;
    private CopyEngine<Writable> keyCopy;
    private CopyEngine<Writable> valueCopy;
    private TwoValueIterable<V> iterable = new TwoValueIterable<V>();
    private boolean combinerEmittedResult = false;

    private boolean lowMemory = false;
    private double lowMemoryThreshold = 0.8;
    private long size = 0;

    private final static int MEMORY_CHECK_FREQUENCY = 100000;

    /**
     * Constructs the combiner.
     *
     * @param params grid writing parameters
     */
    @SuppressWarnings("unchecked")
    public IncrementalMapOutputAccumulator(DataGridWriterParameters<K,V> params) throws IOException, InterruptedException, ClassNotFoundException, NoSuchMethodException {
        super((RunHadoopMapContext<K,V>)params.getMapperContext());
        Configuration configuration = ((RunHadoopMapContext<K,V>)params.getMapperContext()).getConfiguration();
        keyIsWritable = Writable.class.isAssignableFrom(params.getMapperContext().getKeyClass());
        valueIsWritable = Writable.class.isAssignableFrom(params.getMapperContext().getValueClass());
        lowMemoryThreshold = 0.01 * ((double) HServerParameters.getSetting(HServerParameters.MAP_LOW_COMBINING_MEMORY, configuration));
        if (keyIsWritable) {
            Class<? extends Writable> keyClass = ((Class<? extends Writable>) (params.getMapperContext().getKeyClass()));
            keyCopy = new CopyEngine<Writable>(keyClass, configuration);
        }
        if (valueIsWritable) {
            Class<? extends Writable> valueClass = ((Class<? extends Writable>) (params.getMapperContext().getValueClass()));
            valueCopy = new CopyEngine<Writable>(valueClass, configuration);
        }
        gridWriterParameters = params;
    }

    @Override
    public void saveCombineResult(K key, V value) throws IOException, InterruptedException {
        if (currentlyCombiningKey != key && combinerEmittedResult)  //Combiner emitted different key or multiple values per key
        {
            throw new IOException("Supported combiners should emit one or zero values per key and the emitted key should match the incoming key.");
        }

        //If combiner modified the first value that was given to it, our job is done,so we only look at the case when the values do not match.
        if (value != currentlyCombiningValue) {
            if (valueIsWritable) {
                valueCopy.copy((Writable) value, (Writable) currentlyCombiningValue);
            } else {
                keyValueAccumulator.put(currentlyCombiningKey, value);
            }
        }
        currentlyCombiningKey = null;
        currentlyCombiningValue = null;
        combinerEmittedResult = true;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void combine(final K k, final V v) throws IOException, InterruptedException {
        currentlyCombiningValue = keyValueAccumulator.get(k);
        if (currentlyCombiningValue != null) {  //key is already in the accumulator map
            currentlyCombiningKey = k;
                iterable.reset(currentlyCombiningValue, v);
                combinerEmittedResult = false;

                //Call the reducer, which would invoke saveCombinedResult(...) to get us value back
                combinerWrapper.reduce(k, iterable);

                if (!combinerEmittedResult) { //Combiner did not emit anything, remove the key from the map
                    keyValueAccumulator.remove(currentlyCombiningKey);
                }
            currentlyCombiningKey = null;
        } else {
            if (size % MEMORY_CHECK_FREQUENCY == 0) {
                checkMemory();
            }
            size++;

            //If we are above the memory threshold, forward new key-values to the output transport
            //instead of key-value accumulator.
            if (lowMemory) {
                int hadoopPartition = partitionerWrapper.getPartition(k,v);
                gridWriterParameters.setHadoopPartition(hadoopPartition);
                partitions.getGridWriter(gridWriterParameters).put(k, v);
            } else {
                keyValueAccumulator.put(keyIsWritable ? (K) keyCopy.cloneObject((Writable) k) : k, valueIsWritable ? (V) valueCopy.cloneObject((Writable) v) : v);
            }
        }
    }


    @Override
    void mergeInKeyValuesFromAnotherCombiner(WrappingMapOutputAccumulator<K, V> anotherCombiner) throws IOException, InterruptedException {
         if (anotherCombiner instanceof IncrementalMapOutputAccumulator) {
            Map<K, V> otherMap = ((IncrementalMapOutputAccumulator<K, V>) anotherCombiner).keyValueAccumulator;

            for (Map.Entry<K, V> entry : otherMap.entrySet()) {
                combine(entry.getKey(), entry.getValue());
            }

            otherMap.clear();
        }
    }

    @Override
    public void close() throws IOException {
        for (K key : keyValueAccumulator.keySet()) {
            V value = keyValueAccumulator.get(key);
            if (value != null) {
                int hadoopPartition = partitionerWrapper.getPartition(key,value);
                gridWriterParameters.setHadoopPartition(hadoopPartition);
                DataGridChunkedCollectionWriter<K, V> transport = partitions.getGridWriter(gridWriterParameters);
                transport.put(key, value);
            }
        }
        partitions.close();
        keyValueAccumulator = null;
    }

    /**
     * Checks if the JVM is running low on memory.
     */
    private void checkMemory() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        lowMemory = (usedMemory > lowMemoryThreshold * runtime.maxMemory());
    }

}
