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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;


/**
 * A wrapper for {@link BucketSplit}, so it could be used with the
 * "mapred" API.
 */
class BucketSplitMapred extends FileSplit implements GridSplit {
    private static final String[] EMPTY_ARRAY = new String[]{};

    private final BucketSplit wrappedSplit;

    BucketSplitMapred() {
        //Make this split look like FileSplit for Hadoop compatibility
        super(new Path("dummy"), 0l, 0l, EMPTY_ARRAY);
        wrappedSplit = new BucketSplit();
    }

    /**
     * Create the wrapping split.
     *
     * @param wrappedSplit {@link BucketSplit} to wrap
     */
    BucketSplitMapred(BucketSplit wrappedSplit) {
        super(new Path("dummy"), 0l, 0l, EMPTY_ARRAY);
        this.wrappedSplit = wrappedSplit;
    }

    /**
     * Create the wrapping split.
     *
     * @param wrappedSplit {@link BucketSplit} to wrap
     */
    BucketSplitMapred(BucketSplit wrappedSplit, Path dummyPath) {
        super(dummyPath, 0l, 0l, EMPTY_ARRAY);
        this.wrappedSplit = wrappedSplit;
    }

    @Override
    public InetAddress getLocation() {
        return wrappedSplit.getLocation();
    }

    @Override
    public long getLength() {
        try {
            return wrappedSplit.getLength();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] getLocations() throws IOException {
        try {
            return wrappedSplit.getLocations();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        wrappedSplit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        wrappedSplit.readFields(dataInput);
    }

    List<WritableBucketId> getBucketIds() {
        return wrappedSplit.getBucketIds();
    }
}
