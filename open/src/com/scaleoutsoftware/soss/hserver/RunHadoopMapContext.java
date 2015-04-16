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


import com.scaleoutsoftware.soss.hserver.hadoop.MapperWrapper;
import com.scaleoutsoftware.soss.hserver.hadoop.PartitionerWrapper;
import com.scaleoutsoftware.soss.hserver.interop.RunMapContext;
import org.apache.hadoop.conf.Configuration;

/**
 * An immutable context which is passed to all mapper tasks, containing
 * job properties.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class RunHadoopMapContext<K, V> extends RunMapContext<K,V> {
    protected final MapperWrapper mapperWrapper;
    protected final PartitionerWrapper partitionerWrapper;
    protected final Configuration configuration;

    public RunHadoopMapContext(int[] partitionMapping,
                               int invocationId,
                               int outputChunkSizeKb,
                               int hashTableSize,
                               int maxMemoryKb,
                               Class<K> keyClass,
                               Class<V> valueClass,
                               boolean valuesAreFixedLength,
                               int fixedValueLength,
                               MapperWrapper mapperWrapper,
                               PartitionerWrapper partitionerWrapper,
                               Configuration configuration) {
        super(partitionMapping,invocationId,outputChunkSizeKb,hashTableSize,maxMemoryKb,keyClass,valueClass,valuesAreFixedLength,fixedValueLength);
        this.mapperWrapper = mapperWrapper;
        this.partitionerWrapper = partitionerWrapper;
        this.configuration = configuration;
    }

    MapperWrapper getMapperWrapper() {
        return mapperWrapper;
    }

    PartitionerWrapper getPartitionerWrapper() {
        return partitionerWrapper;
    }

    Configuration getConfiguration() {
        return configuration;
    }
}

