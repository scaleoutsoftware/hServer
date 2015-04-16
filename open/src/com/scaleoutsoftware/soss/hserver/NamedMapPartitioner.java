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
import com.scaleoutsoftware.soss.client.map.impl.DefaultSerializer;
import com.scaleoutsoftware.soss.hserver.interop.DataGridChunkedCollectionWriter;
import com.scaleoutsoftware.soss.hserver.interop.HServerConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This partitioner sends the key to the Hadoop partition which is stored on the same host
 * that that the key would be stored at by the named map. This helps to avoid
 * excessive data movement when using {@link GridOutputFormat}.
 */
class NamedMapPartitioner<K, V> extends Partitioner<K, V> {
    private int lookupTable[]; //array index is SOSS key hash, contents is Hadoop partition
    // private CustomSerializer keySerializer = new DefaultSerializer();

    WritableSerializer<Writable> serializer = new WritableSerializer<Writable>(Writable.class);

    public void setLookupTable(int lookupTable[]) {
        if (lookupTable.length != HServerConstants.MAX_MAP_REDUCE_TASKS) {
            throw new RuntimeException("Lookup table does not match");
        }
        this.lookupTable = lookupTable;
    }

    @Override
    public int getPartition(K k, V v, int i) {
        // Use the DataGridChunkedCollectionWriter to get a SOSS region.
        return lookupTable[DataGridChunkedCollectionWriter.selectRegion(serializer, k)];
    }
}
