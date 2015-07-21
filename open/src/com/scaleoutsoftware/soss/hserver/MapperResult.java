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

import com.scaleoutsoftware.soss.client.util.NetUtils;
import com.scaleoutsoftware.soss.hserver.interop.SerializerDeserializer;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This class serves as a result for the mapper PMI invocation.
 * It contains the number of processed  splits and also can
 * be used as a container for the result in case of
 * single result optimisation. In that case it will also
 * retrieve a combiner from the cache to merge the results together.
 */
public class MapperResult<OUTVALUE> implements Serializable {
    private int invocationId;
    private Class<OUTVALUE> resultClass;
    private boolean hasResult = false;
    private transient OUTVALUE result;
    private byte[] serializedResult;
    private int numberOfSplitsProcessed;
    private Map<String, Long> processingTimes = new HashMap<String, Long>();
    private transient SerializerDeserializer<OUTVALUE> serializerDeserializer;

    /**
     * Creates the mapper result object.
     *
     * @param invocationId            invocation id (app id)
     * @param result                  result object
     * @param resultClass             result type
     * @param numberOfSplitsProcessed number of splits processed in the mapper
	 * @param processingTime		  processing time spent in the mapper
     * @throws IOException if result serialization failed
     */
    public MapperResult(int invocationId, OUTVALUE result, Class<OUTVALUE> resultClass, int numberOfSplitsProcessed, long processingTime) throws IOException {
        if (result == null) {
            hasResult = false;
        } else {
            this.result = result;
            this.resultClass = resultClass;
            hasResult = true;
            updateSerializedResult();
        }
        if (numberOfSplitsProcessed == 0 && hasResult) {
            throw new RuntimeException("No splits have been processed, but invocation has a result.");
        }
        this.result = result;
        this.invocationId = invocationId;
        this.numberOfSplitsProcessed = numberOfSplitsProcessed;
        this.processingTimes.put(NetUtils.getLocalHostName(), processingTime);
    }

    /**
     * Merges another result in this result. This method is used to binary
     * merge all the results to the single final result.
     *
     * @param other other result
     * @throws IOException if merge failed
     */
    @SuppressWarnings("unchecked")
    MapperResult mergeWithOther(MapperResult<OUTVALUE> other) throws IOException {
        if (invocationId != other.invocationId) {
            throw new IOException("Attempt to merge two mapper result from different invocations.");
        }
        if (noSplitsWereRun()) {
            return other;
        } else if (other.noSplitsWereRun()) {
            return this;
        }

        //At this point both result object have meaningful information, so we should merge them

        if (hasResult && other.hasResult) { //Both have optimisation results, combine them
            updateDeserializedResult();
            other.updateDeserializedResult();
            SingleKeyMapOutputAccumulator<?, OUTVALUE> combiner = RunMapper.combinerCacheByInvocationId.get(invocationId);
            if (combiner == null) {
                throw new IOException();
            }
            result = combiner.combineTwoValues(result, other.result);
            updateSerializedResult();
        } else if (other.hasResult) //Current MapperResult does not have optimisation result, but the other result does
        {
            other.updateDeserializedResult();
            result = other.result;
            resultClass = other.resultClass; //Copy result class information as well
            hasResult = true;
            updateSerializedResult();
        }

        //If both do not have optimisation results or only current MapperResult has one, nothing has to be done

        numberOfSplitsProcessed += other.numberOfSplitsProcessed;

        processingTimes.putAll(other.processingTimes);

        return this;
    }

    /**
     * Updates the serialized result.
     *
     * @throws IOException if serialization failed
     */
    private void updateSerializedResult() throws IOException {
        if (hasResult) {
            if (serializerDeserializer == null) {
                serializerDeserializer = new SerializerDeserializer<OUTVALUE>(resultClass, null);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serializerDeserializer.serialize(new DataOutputStream(out), result);
            serializedResult = out.toByteArray();
        }
    }

    /**
     * Updates the deserialized result.
     *
     * @throws IOException if deserialization failed
     */
    private void updateDeserializedResult() throws IOException {
        if (result == null) {
            if (serializerDeserializer == null) {
                serializerDeserializer = new SerializerDeserializer<OUTVALUE>(resultClass, null);
            }
            result = serializerDeserializer.deserialize(new DataInputStream(new ByteArrayInputStream(serializedResult)));
        }
    }

    /**
     * Gets the number of splits processed by the mapper.
     *
     * @return number of processed splits
     */
    public int getNumberOfSplitsProcessed() {
        return numberOfSplitsProcessed;
    }

    /**
     * Gets the result object.
     *
     * @return result object
     * @throws IOException if deserialization failed
     */
    public OUTVALUE getResult() throws IOException {
        if (hasResult) {
            updateDeserializedResult();
        }
        return result;
    }

    /**
     * Checks whether this result corresponds to the worker that did not process any splits.
     *
     * @return <code>true</code> if no splits were processed
     */
    boolean noSplitsWereRun() {
        return numberOfSplitsProcessed == 0;
    }

    /**
     * Returns processing times for each host.
     *
     * @return processing times for each host
     */
    Map<String, Long> getProcessingTimes() {
        return processingTimes;
    }

    @Override
    public String toString() {
        return "MapperResult{" +
                "invocationId=" + invocationId +
                ", hasResult=" + hasResult +
                ", result=" + result +
                ", serializedResult=" + Arrays.toString(serializedResult) +
                ", numberOfSplitsProcessed=" + numberOfSplitsProcessed +
                ", processingTimes=" + processingTimes +
                '}';
    }
}
