/*
 Copyright (c) 2013 by ScaleOut Software, Inc.

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


import com.scaleoutsoftware.soss.client.util.SerializationMode;
import com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopInvocationParameters;
import java.net.InetAddress;
import java.util.*;

/**
 * A parameter object, which is passed to the PMI invocation by the {@link JobScheduler}
 *
 * @param <INPUT_SPLIT_BASE_TYPE> base type of the input split, to support both mapred and mapreduce API
 */
public abstract class InvocationParameters<INPUT_SPLIT_BASE_TYPE> {
    protected static final long serialVersionUID = 4L;
    protected HadoopInvocationParameters hadoopInvocationParameters;
    protected int appId;
    protected int[] hadoopPartitionToSossPartitionMapping;
    protected com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostToPartitionsMapping;
    protected int numberOfSlotsPerNode;
    protected Class<? extends INPUT_SPLIT_BASE_TYPE> splitType = null;
    protected List<INPUT_SPLIT_BASE_TYPE> inputSplits;
    protected Map<InetAddress, List<Integer>> inputSplitAssignment = null;
    protected boolean isSingleResultOptimisation = false;
    protected boolean isSortingEnabled = false;
    protected String hadoopVersion;
    protected Object jobParameter;
    protected SerializationMode serializationMode;


    @SuppressWarnings("unused")
    InvocationParameters() {
    }

    public InvocationParameters(HadoopInvocationParameters hadoopInvocationParameters,
                                int appId,
                                int[] hadoopPartitionToSossPartitionMapping,
                                com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostToPartitionsMapping,
                                int numberOfSlotsPerNode,
                                Class<? extends INPUT_SPLIT_BASE_TYPE> splitType,
                                List<INPUT_SPLIT_BASE_TYPE> inputSplits,
                                Map<InetAddress, List<Integer>> inputSplitAssignment,
                                boolean isSingleResultOptimisation,
                                boolean isSortingEnabled,
                                String hadoopVersion,
                                Object jobParameter,
                                SerializationMode serializationMode) {
        this.hadoopInvocationParameters = hadoopInvocationParameters;
        this.appId = appId;
        this.hadoopPartitionToSossPartitionMapping = hadoopPartitionToSossPartitionMapping;
        this.hostToPartitionsMapping = hostToPartitionsMapping;
        this.numberOfSlotsPerNode = numberOfSlotsPerNode;
        this.splitType = splitType;
        this.inputSplits = inputSplits;
        this.inputSplitAssignment = inputSplitAssignment;
        this.isSingleResultOptimisation = isSingleResultOptimisation;
        this.isSortingEnabled = isSortingEnabled;
        this.hadoopVersion = hadoopVersion;
        this.jobParameter = jobParameter;
        this.serializationMode = serializationMode;
    }

    public Object getConfiguration() {
        return hadoopInvocationParameters.getConfiguration();
    }

    public Object getJobId() {
        return hadoopInvocationParameters.getJobID();
    }

    public int[] getHadoopPartitionToSossRegionMapping() {
        return hadoopPartitionToSossPartitionMapping;
    }

    public HostToPartitionsMapping getHostToRegionsMapping() {
        return hostToPartitionsMapping;
    }

    int getNumberOfSlotsPerNode() {
        return numberOfSlotsPerNode;
    }

    public List<INPUT_SPLIT_BASE_TYPE> getInputSplits() {
        return inputSplits;
    }

    Map<InetAddress, List<Integer>> getInputSplitAssignment() {
        return inputSplitAssignment;
    }

    int getAppId() {
        return appId;
    }

    public Object getJobParameter() {
        return jobParameter;
    }

    public boolean isSingleResultOptimisation() {
        return isSingleResultOptimisation;
    }

    boolean isSortingEnabled() {
        return isSortingEnabled;
    }

    public String getHadoopVersion() {
        return hadoopVersion;
    }

    public boolean isOldApi() {
        return hadoopInvocationParameters == null ? false : hadoopInvocationParameters.isOldApi();
    }

    public HadoopInvocationParameters getHadoopInvocationParameters() {
        return hadoopInvocationParameters;
    }

    public SerializationMode getSerializationMode() {
        return this.serializationMode;
    }

    @Override
    public String toString() {
        return "InvocationParameters{" +
                hadoopInvocationParameters+
                ", appId=" + appId +
                ", hadoopPartitionToSossPartitionMapping=" + Arrays.toString(hadoopPartitionToSossPartitionMapping) +
                ", hostToPartitionsMapping=" + hostToPartitionsMapping +
                ", numberOfSlotsPerNode=" + numberOfSlotsPerNode +
                ", splitType=" + splitType +
                ", inputSplits=" + inputSplits +
                ", inputSplitAssignment=" + inputSplitAssignment +
                ", isSingleResultOptimisation=" + isSingleResultOptimisation +
                ", isSortingEnabled=" + isSortingEnabled +
                ", hadoopVersion='" + hadoopVersion + '\'' +
                ", jobParameter=" + jobParameter +
                ", serializationMode=" +serializationMode +
                '}';
    }
}
