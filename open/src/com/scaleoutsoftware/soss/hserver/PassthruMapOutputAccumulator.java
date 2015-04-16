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

import com.scaleoutsoftware.soss.client.NamedCache;
import com.scaleoutsoftware.soss.hserver.interop.DataGridChunkedCollectionWriter;
import com.scaleoutsoftware.soss.hserver.interop.DataGridWriterParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An empty accumulator. This class is used if no combiner is specified for the job. It simply sends
 * key-value pairs to the appropriate partition.
 *
 * @param <K> key type
 * @param <V> value type
 */
class PassthruMapOutputAccumulator<K, V> extends MapOutputAccumulator<K, V> {
    protected final Logger _logger = LoggerFactory.getLogger(NamedCache.class);

    /**
     * Constructs the combiner.
     *
     * @param params context containing job parameters
     */
    public PassthruMapOutputAccumulator(DataGridWriterParameters<K,V> params) throws IOException, InterruptedException, ClassNotFoundException {
        super((RunHadoopMapContext<K,V>)params.getMapperContext());
        gridWriterParameters = params;
    }

    @Override
    public void combine(K k, V v) throws IOException {
        // extract hadoop partition
        int hadoopPartition = partitionerWrapper.getPartition(k,v);
        gridWriterParameters.setHadoopPartition(hadoopPartition);
        DataGridChunkedCollectionWriter<K, V> transport = partitions.getGridWriter(gridWriterParameters);
        transport.put(k, v);
    }

    @Override
    public void close() throws IOException {
        partitions.close();
    }

    @Override
    public  void saveCombineResult(K key, V value) throws IOException, InterruptedException {
      //Do nothing, this is a pass trough accumulator
    }
}
