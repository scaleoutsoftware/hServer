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
import com.scaleoutsoftware.soss.client.ObjectNotSupportedException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of a custom serializer for {@link Writable} instances that can be plugged into a NamedCache by calling
 * {@link com.scaleoutsoftware.soss.client.NamedCache#setCustomSerialization(com.scaleoutsoftware.soss.client.CustomSerializer)}.
 */
public class WritableSerializer<O extends Writable> extends CustomSerializer<O> {
    private final static Map<Class<? extends Writable>, Integer> _writableSizes;

    static
    {
        _writableSizes = new HashMap<Class<? extends Writable>, Integer>();
        _writableSizes.put(IntWritable.class,  4);
        _writableSizes.put(LongWritable.class, 8);
        _writableSizes.put(ByteWritable.class, 1);
        _writableSizes.put(NullWritable.class, 0);
    }

    private int _size = -1;

    //Default constructor for instantiation by PMI.
    WritableSerializer() {
    }

    /**
     * Creates a serializer/deserializer for the objects implementing
     * {@link Writable}.
     *
     * @param objectClass class of the objects to serialize/deserialize
     */
    public WritableSerializer(Class<O> objectClass) {
        super();
        setObjectClass(objectClass);
    }

    /**
     * Serializes an object to <code>OutputStream</code>.
     *
     * @param out output stream
     * @param o   object to be serialized
     * @throws ObjectNotSupportedException if this object cannot be serialized
     * @throws IOException                 if serializer experiences an input/output error
     */
    @Override
    public void serialize(OutputStream out, O o) throws ObjectNotSupportedException, IOException {
        DataOutputStream dataOutputStream;
        if (out instanceof DataOutputStream) {
            dataOutputStream = (DataOutputStream) out;
        } else {
            dataOutputStream = new DataOutputStream(out);
        }
        o.write(dataOutputStream);
    }

    /**
     * Deserializes an object from <code>InputStream</code>.
     *
     * @param in input stream
     * @return deserialized object
     * @throws ObjectNotSupportedException if this object cannot be deserialized
     * @throws IOException                 if serializer experiences an input/output error
     */
    @Override
    public O deserialize(InputStream in) throws ObjectNotSupportedException, IOException, ClassNotFoundException {
        if (objectClass == null) {
            throw new NullPointerException("Object class should be set for WritableSerializer");
        }
        O object = ReflectionUtils.newInstance(objectClass, null);
        return deserialize(in, object);
    }

    /**
     * Deserializes an object from <code>InputStream</code>.
     *
     * @param in             input stream
     * @param instanceToSet  object instance to deserialize into (if possible)
     * @return deserialized object
     * @throws ObjectNotSupportedException if this object cannot be deserialized
     * @throws IOException                 if serializer experiences an input/output error
     */
    @Override
    public O deserialize(InputStream in, O instanceToSet) throws ObjectNotSupportedException, IOException, ClassNotFoundException {
        if (instanceToSet == null) {
            return deserialize(in);
        }
        DataInputStream dataInputStream;
        if (in instanceof DataInputStream) {
            dataInputStream = (DataInputStream) in;
        } else {
            dataInputStream = new DataInputStream(in);
        }
        instanceToSet.readFields(dataInputStream);
        return instanceToSet;
    }

    @Override
    public int getSize() {
        return _size;
    }

    @Override
    public void setObjectClass(Class<O> objectClass) {
        super.setObjectClass(objectClass);
        if(_writableSizes.containsKey(objectClass))
        {
            _size = _writableSizes.get(objectClass);
        }
    }
}
