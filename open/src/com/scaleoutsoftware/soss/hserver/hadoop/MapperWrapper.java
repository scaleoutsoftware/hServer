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
package com.scaleoutsoftware.soss.hserver.hadoop;

import com.scaleoutsoftware.soss.hserver.MapOutputAccumulator;
import com.scaleoutsoftware.soss.hserver.RunHadoopMapContext;
import com.scaleoutsoftware.soss.hserver.interop.RunMapContext;

import java.io.IOException;

/**
 * Abstraction of the mapping side of the hadoop MR job pipeline.
 *
 * @param <INKEY>  mapper input key type
 * @param <INVALUE>  mapper input value type
 * @param <OUTKEY> mapper output key type
 * @param <OUTVALUE> mapper output value type
 */
public interface MapperWrapper<INKEY, INVALUE, OUTKEY, OUTVALUE> {

    /**
     * Run single input split.
     *
     * @param mapOutputAccumulator mapOutputAccumulator
     * @param split
     * @param splitIndex
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    void runSplit(MapOutputAccumulator<OUTKEY, OUTVALUE> mapOutputAccumulator, Object split, int splitIndex)
        throws IOException, ClassNotFoundException, InterruptedException;

    Class<OUTKEY> getMapOutputKeyClass();

    Class<OUTVALUE> getMapOutputValueClass();

    ReducerWrapper<OUTKEY, OUTVALUE, OUTKEY, OUTVALUE> getCombiner(MapOutputAccumulator<OUTKEY, OUTVALUE> consumer, RunMapContext<OUTKEY, OUTVALUE> mapContext) throws IOException, ClassNotFoundException, InterruptedException;

    PartitionerWrapper<OUTKEY, OUTVALUE> getPartitioner();

    boolean hasCombiner();
}
