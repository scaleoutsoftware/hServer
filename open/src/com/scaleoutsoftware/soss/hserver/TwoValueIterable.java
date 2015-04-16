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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@link Iterable} which is passed to the reducer running in the role of combiner.
 * as an argument.
 */
class TwoValueIterable<V> implements Iterable<V> {
    private V v1, v2;
    private int count = 0;
    private Iterator<V> i;

    /**
     * Create the iterable. The values are initialized by calling {@link #reset(Object, Object)}.
     */
    TwoValueIterable() {
        //An iterator for values, always serves two values, first the one already in the map and then the new one.
        i = new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return count < 2;
            }

            @Override
            public V next() {
                count++;
                switch (count) {
                    case 1:
                        return v1;
                    case 2:
                        return v2;
                    default:
                        throw new NoSuchElementException("Iterator is out of values");
                }

            }

            @Override
            public void remove() {
            }
        };
    }

    @Override
    public Iterator<V> iterator() {
        return i;
    }

    public void reset(V v1, V v2) {
        this.v1 = v1;
        this.v2 = v2;
        count = 0;
    }
}
