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

import com.scaleoutsoftware.soss.hserver.hadoop.PartitionerWrapper;
import com.scaleoutsoftware.soss.hserver.interop.*;

import java.io.IOException;

/**
 * This is an abstract base class for map output key-value accumulators.
 * Map output accumulator is the next step after Mapper in the execution pipeline
 * which collects key-value pairs emitted by the mapper. With the exception of the
 * {@link PassthruMapOutputAccumulator} implementations of this class use
 * combiner provided specified in the MR job to compact the output.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class MapOutputAccumulator<K, V>   {
    protected final PartitionerWrapper<K, V> partitionerWrapper;
    protected final int numberPartitions;
    protected PartitionWriters<K, V> partitions;
    protected DataGridWriterParameters<K,V> gridWriterParameters = null;


    /**
     * Constructs the combiner, initializes the pool of data transports for partitions.
     *
     * @param mapperContext context containing job parameters
     * @throws IOException            if errors occurred during the job
     * @throws InterruptedException   if the processing thread is interrupted
     * @throws ClassNotFoundException if the invocation grid does not contain the dependency class
     */
    @SuppressWarnings("unchecked")
    public MapOutputAccumulator(RunHadoopMapContext<K, V> mapperContext) throws IOException, InterruptedException, ClassNotFoundException {
        partitionerWrapper = mapperContext.getPartitionerWrapper();
        numberPartitions = partitionerWrapper.getNumberOfPartitions();
        if (numberPartitions < 0) {
            throw new IllegalArgumentException("Number of partitions should be greater than 0.");
        }
        try {
            if (mapperContext.getPartitionMapping() != null) {  //can be null in case of single key optimisation, than we do no need the transport pool
                partitions = new PartitionWriters<K, V>(mapperContext);
            }
        } catch (NoSuchMethodException e) {
            throw new IOException("Cannot initialize grid data transport", e);
        }
    }

    /**
     * This method is used to pass key-value pairs to the accumulator. It is  called
     * from thew mapper context.
     *
     * @param keyin   key
     * @param valuein value
     * @throws IOException if errors occurred during the job
     * @throws InterruptedException if the processing thread is interrupted
     */
    public abstract void combine(K keyin, V valuein) throws IOException, InterruptedException;

    /**
     * A callback method which is used by reducer context to pass the results from the combiner.
     *
     * @param key   key object
     * @param value value object
     * @throws IOException if errors occurred during the job
     * @throws InterruptedException if the processing thread is interrupted
     */
    public abstract void saveCombineResult(K key, V value) throws IOException, InterruptedException;

    /**
     * This method is used to finalize all IO and is called by mapper context when the mapper is done.
     * @throws IOException if errors occurred during the job
     * @throws InterruptedException if the processing thread is interrupted
     */
    public abstract void close() throws IOException, InterruptedException;
}
