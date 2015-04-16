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

import java.io.IOException;

/**
 * Abstraction of the wrapper for the reduce side of the Hadoop MR pipeline.
 */
public interface ReducerWrapper<INKEY, INVALUE, OUTKEY, OUTVALUE> {
    /**
     * Run reducer.
     */
    void runReducer() throws IOException, InterruptedException;

    /**
     * Perform reduction for a key. Used by {@link com.scaleoutsoftware.soss.hserver.MapOutputAccumulator} to
     * invoke the combiner.
     *
     * @param key    key
     * @param values values for thekey
     */
    void reduce(INKEY key, Iterable<INVALUE> values) throws IOException;
}
