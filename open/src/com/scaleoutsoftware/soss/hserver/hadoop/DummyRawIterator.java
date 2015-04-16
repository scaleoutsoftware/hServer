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
package com.scaleoutsoftware.soss.hserver.hadoop;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.util.Progress;

import java.io.IOException;

/**
 * A dummy iterator, to be supplied to reducer context constructor.
 * None of its methods should be called.
 */
class DummyRawIterator implements RawKeyValueIterator {

    @Override
    public DataInputBuffer getKey() throws IOException {
        return null;
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
        return null;
    }

    @Override
    public boolean next() throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Progress getProgress() {
        return null;
    }
}
