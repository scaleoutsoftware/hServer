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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.*;

/**
 * Wrapper class for input splits, implementing {@link Serializable}.
 * That way they can be saved in the StateServer store.
 */
class SerializableInputSplit implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Configuration conf = new Configuration();
    private static SerializationFactory factory = new SerializationFactory(conf);

    private InputSplit split;  //Wrapped split


    public SerializableInputSplit() {
    }


    //Deserialization from stream
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream aStream) throws IOException, ClassNotFoundException {

        String className = aStream.readUTF();
        Class clazz;
        try {
            clazz = (Class) conf.getClassByName(className);
        } catch (ClassNotFoundException ce) {
            throw new IOException("Split class " + className + " not found", ce);
        }

        Deserializer deserializer = factory.getDeserializer(clazz);
        deserializer.open(aStream);
        split = (InputSplit) deserializer.deserialize(null);

    }

    //Serialization to stream
    @SuppressWarnings("unchecked")
    private void writeObject(ObjectOutputStream aStream) throws IOException {
        aStream.writeUTF(split.getClass().getName());
        Serializer serializer =
                factory.getSerializer(split.getClass());
        serializer.open(aStream);
        serializer.serialize(split);
    }

    /**
     * Serializes the object and returns the resulting byte array.
     *
     * @return byte array representing object
     * @throws IOException if serialization error occurred
     */
    byte[] getBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(byteArrayOutputStream);
        writeObject(os);
        os.flush();
        return byteArrayOutputStream.toByteArray();

    }

    /**
     * Reads the object contents from the byte buffer, setting the wrapped  input split to the saved value..
     *
     * @param buffer a byte array containing object in the serialized form
     * @throws IOException            if deserialization error occurred
     * @throws ClassNotFoundException if deserialization error occurred
     */
    void setObject(byte[] buffer) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        readObject(objectInputStream);
    }

    InputSplit getSplit() {
        return split;
    }

    void setSplit(InputSplit split) {
        this.split = split;
    }
}