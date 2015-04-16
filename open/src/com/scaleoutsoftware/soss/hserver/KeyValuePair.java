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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class bundles key and value together, so they can be treated as single object.
 */
class KeyValuePair<K extends Writable, V extends Writable> {
    private K key;
    private V value;
    private boolean knownKeyValueLength = false;
    private int keySize = 0;
    private int valueSize = 0;

    /**
     * Creates key value pair.
     *
     * @param key   key instance
     * @param value value instance
     */
    KeyValuePair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Create a fixed length key value pair
     *
     * @param key       key instance
     * @param keySize   key length
     * @param value     value instance
     * @param valueSize value length
     */
    KeyValuePair(K key, int keySize, V value, int valueSize) {
        this(key, value);
        if (!(key instanceof Text && value instanceof Text)) {
            throw new IllegalArgumentException("Fixed length keys and values only supported for Text objects.");
        }
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.knownKeyValueLength = true;
    }


    /**
     * Gets key object.
     *
     * @return key
     */
    K getKey() {
        return key;
    }

    /**
     * Gets value object.
     *
     * @return value
     */
    V getValue() {
        return value;
    }

    /**
     * Sets the key instance.
     *
     * @param key key instance
     */
    public void setKey(K key) {
        this.key = key;
    }

    /**
     * Sets the value instance.
     *
     * @param value value instance
     */
    public void setValue(V value) {
        this.value = value;
    }

    /**
     * Reads the next key and value from the underlying buffer.
     *
     * @param dataInput input stream representation of the buffer
     * @param buffer    buffer
     * @throws IOException if the read from the buffer failed
     */
    void readNext(DataInputStream dataInput, ByteBuffer buffer) throws IOException {
        if (!knownKeyValueLength) {
            key.readFields(dataInput);
            value.readFields(dataInput);
        } else {
            ((Text) key).set(buffer.array(), buffer.position(), keySize);
            buffer.position(buffer.position() + keySize);
            ((Text) value).set(buffer.array(), buffer.position(), valueSize);
            buffer.position(buffer.position() + valueSize);
        }

    }

    /**
     * Writes the key and value to the output stream.
     *
     * @param dataOutput output stream
     * @throws IOException if write failed
     */
    public void write(DataOutputStream dataOutput) throws IOException {
        if (!knownKeyValueLength) {
            key.write(dataOutput);
            value.write(dataOutput);
        } else {
            byte[] keyB = ((Text) key).getBytes();
            byte[] valB = ((Text) value).getBytes();
            if (keyB.length != keySize || valB.length != valueSize)
                throw new IOException("Wrong key/value size " + keyB.length + "," + valB.length);
            dataOutput.write(keyB);
            dataOutput.write(valB);
        }
    }

}
