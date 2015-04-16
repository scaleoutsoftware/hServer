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

import org.apache.hadoop.io.Writable;

import java.io.IOException;

/**
 * This accumulator is used for single result optimisation, when mapper/combiner output key space
 * consists of single key only.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SingleKeyMapOutputAccumulator<K, V> extends WrappingMapOutputAccumulator<K, V> {
    private K key;
    private V aggregateValue = null;
    private boolean valueIsWritable;
    private CopyEngine<Writable> valueCopy;
    private TwoValueIterable<V> iterable = new TwoValueIterable<V>();
    private boolean combinerResultExpected = false;

    /**
     * Constructs the combiner.
     */
    @SuppressWarnings("unchecked")
    public SingleKeyMapOutputAccumulator(RunHadoopMapContext<K, V> mapContext) throws IOException, InterruptedException, ClassNotFoundException, NoSuchMethodException {
        super(mapContext);
        valueIsWritable = Writable.class.isAssignableFrom(mapContext.getValueClass());
        if (valueIsWritable) {
            valueCopy = new CopyEngine<Writable>((Class<Writable>) mapContext.getValueClass(), mapContext.getConfiguration());
        }
    }

    @Override
    public void saveCombineResult(K keyin, V valuein) throws IOException, InterruptedException {
        if (!combinerResultExpected) {
            throw new IOException("Combiner can emit only one key-value pair in the single result optimisation. ");
        }
        if (!key.equals(keyin)) {
            throw new IOException("All map output keys should be the same in the single result optimisation. Keys " + key + " and " + keyin + " do not match.");
        }
        if (valuein != aggregateValue)  //If the first value was modified by the reducer value was already modified, our job is done
        {
            aggregateValue = valuein;
        }
        combinerResultExpected = false;
    }

    @Override
    void mergeInKeyValuesFromAnotherCombiner(WrappingMapOutputAccumulator<K, V> anotherCombiner) throws IOException, InterruptedException {
        if (anotherCombiner instanceof SingleKeyMapOutputAccumulator) {
            V value = ((SingleKeyMapOutputAccumulator<K, V>) anotherCombiner).aggregateValue;
            K key = ((SingleKeyMapOutputAccumulator<K, V>) anotherCombiner).key;
            if (value != null) {
                if (key == null) {
                    throw new IOException("Key cannot be null.");
                }
                combine(key, value);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void combine(K keyin, V valuein) throws IOException, InterruptedException {
        if (aggregateValue == null) {
            if (keyin == null) {
                throw new IOException("Key cannot be null. Value = " + valuein);
            }
            aggregateValue = valueIsWritable ? (V) valueCopy.cloneObject((Writable) valuein) : valuein;
            key = keyin;
        } else {
            if (key == null || !key.equals(keyin)) {
                throw new IOException("All map output keys should be the same in case of single result optimisation. Keys " + key + " and " + keyin + " do not match.");
            }
            iterable.reset(aggregateValue, valuein);
            combinerResultExpected = true;
            combinerWrapper.reduce(key, iterable);
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
    }

    /**
     * This method combines two values together. It ignores and erases any previous value
     * combiner may have held, so it is only used at the result merge stage of the single result
     * optimization.
     *
     * @param v1 first value
     * @param v2 second value
     * @return combined value
     */
    V combineTwoValues(V v1, V v2) throws IOException {
        aggregateValue = null;
        iterable.reset(v1, v2);
        combinerResultExpected = true;
        combinerWrapper.reduce(key, iterable);
        return aggregateValue;
    }

    /**
     * Get the value accumulated in the combiner.
     *
     * @return the aggregate value
     */
    V getAggregateValue() {
        return aggregateValue;
    }
}
