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


import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.client.map.impl.ChunkBufferPoolFactory;
import com.scaleoutsoftware.soss.client.map.impl.ServerConnectionPool;
import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import com.scaleoutsoftware.soss.hserver.interop.ChunkedCollectionWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * This class is used to write objects to the bucket in the StateServer storage. It combines objects into chunks
 * to prevent memory overhead.
 *
 * @see BucketReader
 */
class BucketWriter<K extends Writable, V extends Writable> extends ChunkedCollectionWriter<K, V> {
    private final KeyValuePair<K, V> keyValuePair;
    private final BucketStore bucketStore;
    private final BucketId BucketId;
    private final int maxNumberPendingChunks;
    private final ServerConnectionPool connectionPool;
    private final static String writerId = "BucketWriter";

    /**
     * Creates the bucket writer.
     *
     * @param bucketStore   bucket store
     * @param BucketId      bucket id
     * @param configuration configuration
     * @param keyValuePair  key and value objects to be used
     * @throws StateServerException in case there was an error while communicating to StateServer
     */
    BucketWriter(BucketStore bucketStore, BucketId BucketId, Configuration configuration, KeyValuePair<K, V> keyValuePair) throws IOException, StateServerException {
        super(
                ChunkBufferPoolFactory.createOrGetBufferPool(
                        BucketWriter.class,
                        Math.max(HServerParameters.getSetting(HServerParameters.CACHE_MAXTEMPMEMORY_KB, configuration) / HServerParameters.getSetting(HServerParameters.CACHE_CHUNKSIZE_KB, configuration), 1),
                        1024* HServerParameters.getSetting(HServerParameters.CACHE_CHUNKSIZE_KB, configuration), false)
              );
        this.bucketStore = bucketStore;
        this.BucketId = BucketId;
        this.keyValuePair = keyValuePair;
        this.connectionPool = ServerConnectionPool.getInstance();
        this.maxNumberPendingChunks = chunkBufferPool.getMaxNumberOfBuffersToUse();
        bucketStore.clearBucket(BucketId);
    }


    @Override
    protected void putObject(DataOutputStream dataOutput, int entryNumber, K k, V v) throws IOException {
        keyValuePair.setKey(k);
        keyValuePair.setValue(v);
        keyValuePair.write(dataOutput);
    }

    @Override
    protected void writeChunkOut(ByteBuffer chunk, int chunkIndex, int numberOfEntries) throws IOException {
        try {
            bucketStore.putObject(BucketId, chunkIndex, chunk.array(), chunk.limit());
        } catch (StateServerException e) {
            throw new IOException("Cannot write data to the data grid.", e);
        }
    }

    @Override
    protected Future scheduleAsyncWrite(Callable<Object> writeOperation) {
        return connectionPool.scheduleLocalOperation(writerId, writeOperation, maxNumberPendingChunks);
    }
}

