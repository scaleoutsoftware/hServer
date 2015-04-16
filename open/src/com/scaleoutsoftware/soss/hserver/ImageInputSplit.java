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

import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.client.util.NetUtils;
import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Random;

/**
 * This input split is part of the {@link GridImage}. It is wrapper over original input split containing
 * additional information which allow to read it from the bucket store.
 *
 * {@link GridImage}
 */
class ImageInputSplit extends InputSplit implements Serializable, Writable, GridSplit {
    private static final long serialVersionUID = 1L;

    private SerializableInputSplit fallbackInputSplit = new SerializableInputSplit();
    private String imageIdString;
    private long imageCreationTimestamp;
    private int splitIndex;

    private BucketId BucketId = new BucketId();
    private int numberOfKeys;
    private int numberOfChunks;
    private String keyClass = "";
    private String valueClass = "";
    private String recordedHostName = "";
    private String taskId = "";
    private boolean isRecorded = false;


    //Used for serialization
    @SuppressWarnings("unused")
    public ImageInputSplit() {
    }

    ImageInputSplit(InputSplit fallbackInputSplit, String imageIdString, long imageCreationTimestamp, int splitIndex) {
        this.fallbackInputSplit.setSplit(fallbackInputSplit);
        this.imageIdString = imageIdString;
        this.imageCreationTimestamp = imageCreationTimestamp;
        this.splitIndex = splitIndex;

    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return fallbackInputSplit.getSplit().getLength();
    }


    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        if (recordedHostName != null && recordedHostName.length() > 0) {
            return new String[]{recordedHostName};
        }
        if (isRecorded()) {
            try {
                InetAddress location = BucketStore.getBucketLocation(BucketId);
                if (location == null) {
                    return fallbackInputSplit.getSplit().getLocations();
                }
                return new String[]{location.getHostName()};
            } catch (StateServerException e) {
                throw new IOException("Cannot access ScaleOut StateServer", e);
            }
        } else {
            if (fallbackInputSplit.getSplit().getLocations().length > 1) {
                int index = (new Random(System.currentTimeMillis())).nextInt(fallbackInputSplit.getSplit().getLocations().length);
                //We return only one location to discourage running split concurrently and randomize
                //indexes to achieve better distribution.
                return new String[]{fallbackInputSplit.getSplit().getLocations()[index]};
            } else {
                return fallbackInputSplit.getSplit().getLocations();
            }
        }
    }

    @Override
    public InetAddress getLocation() {
        if (isRecorded()) {
            try {
                return BucketStore.getBucketLocation(BucketId);
            } catch (StateServerException e) {
                //Will return null
            }
        }
        return null;
    }

    @Override
    public int hashCode() {
        return fallbackInputSplit.getSplit().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ImageInputSplit &&
                fallbackInputSplit.getSplit().equals(((ImageInputSplit) obj).fallbackInputSplit.getSplit());

    }

    @Override
    public String toString() {
        return "ImageInputSplit[ originalSplit = " + fallbackInputSplit.getSplit() +
                "; imageIdString = " + imageIdString +
                "; imageCreationTime = " + imageCreationTimestamp +
                "; splitIndex = " + splitIndex +
                "; isRecorded = " + isRecorded +
                "; BucketId = " + BucketId +
                "; numberOfKeys = " + numberOfKeys +
                "; numberOfChunks = " + numberOfChunks + "]";

    }

    //Writable serialization/deserialization methods
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        byte splitBytes[] = fallbackInputSplit.getBytes();

        dataOutput.writeUTF(imageIdString);
        dataOutput.writeLong(imageCreationTimestamp);
        dataOutput.writeInt(splitIndex);
        ((WritableBucketId)BucketId).write(dataOutput);
        dataOutput.writeInt(numberOfKeys);
        dataOutput.writeInt(numberOfChunks);
        dataOutput.writeUTF(keyClass);
        dataOutput.writeUTF(valueClass);
        dataOutput.writeUTF(recordedHostName);
        dataOutput.writeUTF(taskId);
        dataOutput.writeBoolean(isRecorded);
        dataOutput.writeInt(splitBytes.length);
        dataOutput.write(splitBytes);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        imageIdString = dataInput.readUTF();
        imageCreationTimestamp = dataInput.readLong();
        splitIndex = dataInput.readInt();
        ((WritableBucketId)BucketId).readFields(dataInput);
        numberOfKeys = dataInput.readInt();
        numberOfChunks = dataInput.readInt();
        keyClass = dataInput.readUTF();
        valueClass = dataInput.readUTF();
        recordedHostName = dataInput.readUTF();
        taskId = dataInput.readUTF();
        isRecorded = dataInput.readBoolean();
        byte splitBytes[] = new byte[dataInput.readInt()];
        dataInput.readFully(splitBytes);
        try {
            fallbackInputSplit.setObject(splitBytes);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    //Getters and setters for properties.

    InputSplit getFallbackInputSplit() {
        return fallbackInputSplit.getSplit();
    }

    BucketId getBucketId() {
        return BucketId;
    }

    int getNumberOfKeys() {
        return numberOfKeys;
    }


    String getImageIdString() {
        return imageIdString;
    }


    long getImageCreationTimestamp() {
        return imageCreationTimestamp;
    }


    void setNumberOfKeys(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    int getNumberOfChunks() {
        return numberOfChunks;
    }

    void setNumberOfChunks(int numberOfChunks) {
        this.numberOfChunks = numberOfChunks;
    }

    void setKeyValueClass(Class<? extends Writable> key, Class<? extends Writable> value) {
        this.keyClass = key.getName();
        this.valueClass = value.getName();
    }

    String getKeyClass() {
        return keyClass;
    }

    String getValueClass() {
        return valueClass;
    }

    void setBucketId(BucketId BucketId) {
        this.BucketId = BucketId;
    }


    boolean isRecorded() {
        return isRecorded;
    }

    void setRecorded(boolean recorded) {
        isRecorded = recorded;
        if (isRecorded) {
            try {
                recordedHostName = NetUtils.getLocalHostName();
            } catch (Exception e) {
                //Do nothing, optimization
            }
        }
    }

    int getSplitIndex() {
        return splitIndex;
    }

    /**
     * Sets the task ID, which is used to determine if the split is being recorded somewhere else.
     */
    String getTaskId() {
        return taskId;
    }

    void setTaskId(String taskId) {
        this.taskId = taskId;
    }

}

