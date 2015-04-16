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

import com.scaleoutsoftware.soss.client.da.DataAccessor;
import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.client.da.StateServerKey;
import com.scaleoutsoftware.soss.client.map.impl.ChunkBuffer;
import com.scaleoutsoftware.soss.client.map.impl.ChunkBufferPoolFactory;
import com.scaleoutsoftware.soss.client.map.impl.ServerConnectionPool;
import com.scaleoutsoftware.soss.client.messaging.messages.ReadObjectReply;
import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import com.scaleoutsoftware.soss.hserver.interop.ChunkedCollectionReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;


/**
 * This class is used to read objects from the chunks contained in the bucket .
 *
 * @see BucketWriter
 */
class BucketReader<K extends Writable, V extends Writable> extends ChunkedCollectionReader<K, V, StateServerKey> {
    private final KeyValuePair<K, V> keyValuePair;
    private final LinkedList<StateServerKey> chunkKeysList;
    private final ServerConnectionPool connectionPool;

    private final static String readerId = "BucketReader";


    /**
     * Creates the reader.
     *
     * @param bucketStore    bucket store
     * @param BucketId       bucket id
     * @param numberOfChunks number of chunks in the bucket
     * @param configuration  configuration
     * @param keyValuePair   key and value objects to be used
     * @throws IOException if the bucket key list cannot be read or its size does not match the number of chunks
     */
    BucketReader(BucketStore bucketStore,
                 BucketId BucketId,
                 int numberOfChunks,
                 Configuration configuration,
                 KeyValuePair<K, V> keyValuePair) throws IOException {
        super(ChunkBufferPoolFactory.createOrGetBufferPool(BucketReader.class,
                Math.max(HServerParameters.getSetting(HServerParameters.CACHE_MAXTEMPMEMORY_KB, configuration) / HServerParameters.getSetting(HServerParameters.CACHE_CHUNKSIZE_KB, configuration), 1),
                1024* HServerParameters.getSetting(HServerParameters.CACHE_CHUNKSIZE_KB, configuration),
                false),
                HServerParameters.getSetting(HServerParameters.REDUCE_CHUNKREADTIMEOUT, configuration));

        this.keyValuePair = keyValuePair;
        try {
            chunkKeysList = bucketStore.getBucketContents(BucketId);
        } catch (StateServerException e) {
            throw new IOException("Cannot retrieve bucket contents.", e);
        }
        if (chunkKeysList.size() != numberOfChunks) {
            throw new IOException("Recorded split is corrupted. " + chunkKeysList.size() + "-" + numberOfChunks);
        }
        this.connectionPool = ServerConnectionPool.getInstance();
    }

    @Override
    protected StateServerKey nextChunkIdentifier() throws IOException {
        return chunkKeysList.poll();
    }

    @Override
    protected void readChunkFromTheGrid(StateServerKey stateServerKey, ChunkBuffer readInto, int chunkIndex) throws IOException {
        DataAccessor da = new DataAccessor(stateServerKey);
        da.setLockedWhenReading(false);
        ReadObjectReply reply;
        try {
            reply = da.readIntoBuffer(readInto.getBuffer().array(), true, false);
        } catch (StateServerException e) {
            throw new IOException("Error while reading object form StateServer", e);
        }
        int length = reply.getDataLength();
        if (length < 0) {
            throw new IOException("Error while reading object form StateServer");
        }
        readInto.getBuffer().limit(reply.getDataLength());
    }

    @Override
    protected boolean readNextFromCurrentChunk(StateServerKey key, DataInputStream dataInput, ByteBuffer buffer, int chunkIndex) throws IOException {
        keyValuePair.readNext(dataInput, buffer);
        return true;
    }

    @Override
    public K getKey() {
        return keyValuePair.getKey();
    }

    @Override
    public V getValue() {
        return keyValuePair.getValue();
    }

    @Override
    protected Future<Object> scheduleAsyncRead(Callable<Object> writeOperation) {
        return connectionPool.scheduleLocalOperation(readerId, writeOperation);
    }
}
