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


import com.scaleoutsoftware.soss.client.CachedObjectId;
import com.scaleoutsoftware.soss.client.CustomSerializer;
import com.scaleoutsoftware.soss.client.da.StateServerKey;
import com.scaleoutsoftware.soss.client.messaging.messages.ObjectArray;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import com.scaleoutsoftware.soss.hserver.interop.ObjectReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Contains the logic for asynchronous prefetching of the objects from StateServer store. It allows to have
 * multiple objects which are read-ahead and are ready to be served. The prefetcher maintains a pool of reusable
 * object descriptors, so it does not stress the GC by generating a constant stream of throw-away objects.
 */
class ObjectPrefetcher<T> {
    //Used object descriptors
    final private ArrayBlockingQueue<Object> readyToBeServed;

    //Unused object descriptors
    final private ArrayBlockingQueue<ObjectDescriptor<T>> unused;

    //Last served descriptor, which is returned to the unused pool,
    // once the request for the next object  is made
    private ObjectDescriptor<T> currentObject = null;


    private volatile Exception exception = null;

    //This object used to denote end of the clazz objects list
    private final Object endToken = new Object();

    final boolean isWritable;
    final Class<T> objectClass;
    final Configuration configuration;
    final List<StateServerKey> keys;
    final CustomSerializer serializer;
    final int initialSize;

    /**
     * Creates the prefetcher.
     *
     * @param keys             the list of IDs corresponding to objects
     * @param prefetchingDepth number of objects to prefetch
     * @param objectClass      object class
     * @param configuration    configuration
     * @param initialSize      initial size of the reusable prefetching buffer
     */
    ObjectPrefetcher(final List<StateServerKey> keys, int prefetchingDepth, Class<T> objectClass, CustomSerializer serializer, Configuration configuration, int initialSize) throws IOException {
        readyToBeServed = new ArrayBlockingQueue<Object>(prefetchingDepth + 1);
        unused = new ArrayBlockingQueue<ObjectDescriptor<T>>(prefetchingDepth);

        ObjectDescriptor<T> object;
        for (int i = 0; i < prefetchingDepth; i++) {
            object = new ObjectDescriptor<T>();
            try {
                unused.put(object);
            } catch (InterruptedException e) {
                exception = e;
            }
        }

        isWritable = Writable.class.isAssignableFrom(objectClass);
        this.objectClass = objectClass;
        this.configuration = configuration;
        this.keys = keys;
        this.initialSize = initialSize;
        this.serializer = serializer;
    }

    @SuppressWarnings("unchecked")
    void startPrefetching() {
        new Thread(new Runnable() {

            public void run() {
                try {
                    ObjectReader reader = BucketStore.getObjectReader(initialSize);
                    DataInputBuffer buffer = new DataInputBuffer();
                    for (StateServerKey id : keys) {
                        reader.read(id);

                        ObjectDescriptor<T> objectDescriptor = unused.take();
                        objectDescriptor.key = new CachedObjectId<T>(id);

                        if (isWritable) {
                            buffer.reset(reader.getBuffer(), reader.getLength());
                            if (objectDescriptor.object == null) {
                                objectDescriptor.object = ReflectionUtils.newInstance(objectClass, configuration);
                            }
                            ((Writable) objectDescriptor.object).readFields(buffer);
                        } else {
                            objectDescriptor.object = (T) ObjectArray.deserialize(reader.getBuffer(), 0, reader.getLength(), serializer);
                        }
                        readyToBeServed.put(objectDescriptor);
                    }
                    readyToBeServed.put(endToken);
                } catch (Exception e) {
                    //Save the exception to be later rethrown by next()
                    exception = e;
                }
            }
        }).start();
    }

    /**
     * Reads the next object, also triggering the return of
     * previously served object descriptor to the unused pool. In case
     * of Writable objects this means that the previously served object
     * fields could be reset, so the application should be done with it.
     *
     * @return <code>true</code> if next object was successfully read
     * @throws Exception to re-throw exceptions which occurred during reading
     */
    @SuppressWarnings("unchecked")
    boolean readNext() throws Exception {
        if (exception != null) throw exception;
        if (currentObject != null) unused.put(currentObject);
        Object next = readyToBeServed.take();
        if (next == endToken) {
            return false;
        } else {
            currentObject = (ObjectDescriptor<T>) next;
            return true;
        }
    }

    /**
     * Returns the next object.
     *
     * @return next object
     */
    T nextObject() {
        return currentObject.object;
    }

    /**
     * Returns the ID of the next object.
     *
     * @return next object ID
     */
    CachedObjectId<T> nextKey() {
        return currentObject.key;
    }

    /*
     * Stores object and its key together.
     */
    private class ObjectDescriptor<T> {
        CachedObjectId<T> key;
        T object;
    }

}
