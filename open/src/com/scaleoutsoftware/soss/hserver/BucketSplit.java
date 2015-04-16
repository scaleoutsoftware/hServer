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


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


/**
 * An input split that corresponds to the contents of the one or several buckets.
 */
class BucketSplit extends InputSplit  implements Writable, Serializable, GridSplit {
    private static final long serialVersionUID = 1L;

    private List<WritableBucketId> ids;
    private InetAddress location;

    /**
     * Creates a split, based on list of bucket IDs.
     *
     * @param ids       list of bucket IDs
     * @param locations "home" location of the split
     */
    BucketSplit(List<WritableBucketId> ids, InetAddress locations) {
        if(ids==null || locations ==null)
        {
            throw new IllegalArgumentException("Illegal split definition:"+ids+","+locations);
        }
        this.ids = ids;
        this.location = locations;
    }

    @SuppressWarnings("unused")
    BucketSplit() {
    }

    List<WritableBucketId> getBucketIds() {
        return ids;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        //Making the assumption that each bucket contains roughly the same number of keys,
        //so the splits can be compared by comparing number of buckets.
        return ids.size();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{location.getHostName()};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        byte addr[] = location.getAddress();
        dataOutput.writeInt(addr.length);
        dataOutput.write(location.getAddress());
        dataOutput.writeInt(ids.size());
        for (WritableBucketId id : ids) {
            id.write(dataOutput);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int addrLength = dataInput.readInt();
        byte addr[] = new byte[addrLength];
        dataInput.readFully(addr);
        location = InetAddress.getByAddress(addr);
        int idSize = dataInput.readInt();
        ids = new ArrayList<WritableBucketId>(idSize);
        for (int i = 0; i < idSize; i++) {
            WritableBucketId id = new WritableBucketId();
            id.readFields(dataInput);
            ids.add(id);
        }
    }

    @Override
    public InetAddress getLocation() {
        return location;
    }
}
