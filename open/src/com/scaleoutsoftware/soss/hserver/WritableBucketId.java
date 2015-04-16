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

import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

/**
 * This class wraps a BucketId to implement Writable (Hadoop specific code)
 */
public class WritableBucketId extends BucketId implements Serializable, Writable {

    WritableBucketId() {
        super();
    }

    WritableBucketId(int bucketNumber, int appId) {
        super(bucketNumber,appId);
    }

    public static WritableBucketId copy(BucketId copy) {
        return new WritableBucketId(copy.getBucketNumber(), copy.getAppId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(bucketNumber);
        dataOutput.writeInt(appId);
        dataOutput.writeLong(id.getMostSignificantBits());
        dataOutput.writeLong(id.getLeastSignificantBits());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bucketNumber = dataInput.readInt();
        appId = dataInput.readInt();
        id = new UUID(dataInput.readLong(), dataInput.readLong());
    }
}
