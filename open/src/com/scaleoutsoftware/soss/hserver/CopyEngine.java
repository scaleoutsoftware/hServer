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


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * This class contains helper methods for copying Writables.
 *
 * @param <T> object type
 */
class CopyEngine<T extends Writable> {

    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

    private Configuration configuration;
    private WritableCopy copy;
    private Constructor<? extends T> constructor;
    private boolean isConfigurable;

    /**
     * Create the copy engine.
     */
    public CopyEngine(Class<? extends T> clazz, Configuration configuration) throws NoSuchMethodException {
        this.configuration = configuration;
        copy = WritableCopy.getForClass(clazz, configuration);
        constructor = clazz.getDeclaredConstructor(EMPTY_ARRAY);
        constructor.setAccessible(true);
        isConfigurable = Configurable.class.isAssignableFrom(clazz);

    }

    /**
     * Creates an object clone.
     *
     * @param orig object to clone
     * @return cloned instance
     */
    T cloneObject(T orig) throws IOException {
        if (orig == null){
            return null;
        }
        T newInst = newInstance();
        copy.copy(orig, newInst);
        return newInst;

    }

    /**
     * Copy object from source to destination
     *
     * @param src source
     * @param dst destination
     */
    void copy(T src, T dst) throws IOException {
        copy.copy(src, dst);
    }


    /**
     * Creates new instance of the underlying class.
     */
    private T newInstance() {
        T result;
        try {
            result = constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (isConfigurable && configuration != null) {
            ((Configurable) result).setConf(configuration);
        }
        return result;
    }


    /**
     * This class is used to specify custom copying behaviour for various Writables. Since incremental combiner relies
     * heavily on object copying, an effort is made to optimise it for often used types.
     */
    private abstract static class WritableCopy {
        abstract Object copy(Object src, Object dst) throws IOException;

        static WritableCopy getForClass(Class clazz, final Configuration configuration) {
            if (IntWritable.class.isAssignableFrom(clazz)) {
                return new WritableCopy() {
                    @Override
                    Object copy(Object src, Object dst) throws IOException {
                        ((IntWritable) dst).set(((IntWritable) src).get());
                        return dst;
                    }
                };
            } else if (LongWritable.class.isAssignableFrom(clazz)) {
                return new WritableCopy() {
                    @Override
                    Object copy(Object src, Object dst) throws IOException {
                        ((LongWritable) dst).set(((LongWritable) src).get());
                        return dst;
                    }
                };
            } else if (Text.class.isAssignableFrom(clazz)) {
                return new WritableCopy() {
                    @Override
                    Object copy(Object src, Object dst) throws IOException {
                        ((Text) dst).set((Text) src);
                        return dst;
                    }
                };
            } else if (Writable.class.isAssignableFrom(clazz)) {
                return new WritableCopy() {
                    @Override
                    Object copy(Object src, Object dst) throws IOException {
                        ReflectionUtils.copy(configuration, src, dst);
                        return dst;
                    }
                };
            } else {
                return new WritableCopy() {
                    @Override
                    Object copy(Object src, Object dst) throws IOException {
                        throw new RuntimeException("Key/Value should be Writable");
                    }
                };

            }

        }

    }


}
