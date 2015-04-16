/*
 Copyright (c) 2013 by ScaleOut Software, Inc.

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

import com.scaleoutsoftware.soss.hserver.hadoop.ReducerWrapper;

import java.io.IOException;

/**
 * This abstract class represents a {@link MapOutputAccumulator} wrapped around Hadoop
 * combiner class.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class WrappingMapOutputAccumulator<K, V> extends MapOutputAccumulator<K, V> {
    protected final ReducerWrapper<K,V,K,V> combinerWrapper;

    /**
     * Constructs the combiner.
     *
     * @param mapContext map context
     */
    @SuppressWarnings("unchecked")
    WrappingMapOutputAccumulator(RunHadoopMapContext<K, V> mapContext) throws IOException, InterruptedException, ClassNotFoundException {
        super(mapContext);
        combinerWrapper = mapContext.getMapperWrapper().getCombiner(this, mapContext);
    }

    /**
     * Merges the results accumulated in another combiner to this combiner
     *
     * @param anotherCombiner combiner to merge results from
     */
    abstract void mergeInKeyValuesFromAnotherCombiner(WrappingMapOutputAccumulator<K, V> anotherCombiner) throws IOException, InterruptedException;

}
