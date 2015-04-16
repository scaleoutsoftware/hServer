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

import com.scaleoutsoftware.soss.hserver.interop.SerializerDeserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class WritableSerializerDeserializer<T> extends SerializerDeserializer<T> {
    final boolean isWritable;

    /**
     * Construct a serializer/deserializer
     *
     * @param objectType type of the objects for serialization/deserialization
     */
    public WritableSerializerDeserializer(Class<T> objectType) {
        super(objectType);
        isWritable = Writable.class.isAssignableFrom(objectType);
        if (isWritable) {
            object = ReflectionUtils.newInstance(objectType, null);
        }
    }


    /**
     * Serialize object.
     *
     * @param input input stream to read object from
     * @return deserialized object
     */
    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(DataInputStream input) throws IOException {
        if (isWritable) {
            ((Writable) object).readFields(input);
            return object;
        } else {
            return super.deserialize(input);
        }
    }

    /**
     * Serialize object.
     *
     * @param out    output stream to write object to
     * @param object object to be serialized
     */
    @Override
    public void serialize(DataOutputStream out, T object) throws IOException {
        if (isWritable) {
            ((Writable) object).write(out);
        } else {
            super.serialize(out, object);
        }
    }
}
