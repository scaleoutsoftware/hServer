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


import com.scaleoutsoftware.soss.client.pmi.InvocationGridLocalCache;
import com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping;
import com.scaleoutsoftware.soss.hserver.interop.HServerConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

/**
 * A parameter object, which is passed to the PMI invocation by the {@link JobScheduler}
 *
 * @param <INPUT_SPLIT_BASE_TYPE> base type of the input split, to support both mapred and mapreduce API
 */
public class InvocationParameters<INPUT_SPLIT_BASE_TYPE> implements Serializable {
    private static final long serialVersionUID = 4L;

    //LRU map for the invocation parameters. The invocation parameters are put in this map
    //by at the map invocation step, to be retrieved at the reduce step.
    private final static Map<Integer, InvocationParameters> parameterCacheByInvocationId = Collections.synchronizedMap(new LinkedHashMap<Integer, InvocationParameters>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry eldest) {
            return super.size() > HServerConstants.MAX_SIMULTANEOUS_JOBS;
        }
    });


    private Configuration configuration;
    private Class<? extends Configuration> configurationClass;
    private JobID jobId;
    private int appId;
    private int[] hadoopPartitionToSossRegionMapping;
    private com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostToPartitionsMapping;
    private int numberOfSlotsPerNode;
    private Class<? extends INPUT_SPLIT_BASE_TYPE> splitType = null;
    private List<INPUT_SPLIT_BASE_TYPE> inputSplits;
    private Map<InetAddress, List<Integer>> inputSplitAssignment = null;
    private boolean isSingleResultOptimisation = false;
    private boolean isSortingEnabled = false;
    private boolean isOldApi = false;
    private String hadoopVersion;
    private Object jobParameter;

    @SuppressWarnings("unused")
    InvocationParameters() {
    }

    InvocationParameters(Configuration configuration,
                         JobID jobId,
                         int appId,
                         int[] hadoopPartitionToSossRegionMapping,
                         com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostToPartitionsMapping,
                         int numberOfSlotsPerNode,
                         Class<? extends INPUT_SPLIT_BASE_TYPE> splitType,
                         List<INPUT_SPLIT_BASE_TYPE> inputSplits,
                         Map<InetAddress, List<Integer>> inputSplitAssignment,
                         boolean isSingleResultOptimisation,
                         boolean isSortingEnabled,
                         boolean isOldApi,
                         String hadoopVersion,
                         Object jobParameter) {
        this.configuration = configuration;
        this.configurationClass = configuration.getClass();
        this.jobId = jobId;
        this.appId = appId;
        this.hadoopPartitionToSossRegionMapping = hadoopPartitionToSossRegionMapping;
        this.hostToPartitionsMapping = hostToPartitionsMapping;
        this.numberOfSlotsPerNode = numberOfSlotsPerNode;
        this.splitType = splitType;
        this.inputSplits = inputSplits;
        this.inputSplitAssignment = inputSplitAssignment;
        this.isSingleResultOptimisation = isSingleResultOptimisation;
        this.isSortingEnabled = isSortingEnabled;
        this.isOldApi = isOldApi;
        this.hadoopVersion = hadoopVersion;
        this.jobParameter = jobParameter;
    }


    private void writeObject(java.io.ObjectOutputStream outstream)
            throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(CompressingStreamsFactory.getCompressedOutputStream(outstream));
        out.writeBoolean(isOldApi);
        out.writeObject(configurationClass);
        configuration.write(out);
        jobId.write(out);
        out.writeObject(jobParameter);
        out.writeInt(appId);
        out.writeObject(hadoopPartitionToSossRegionMapping);
        out.writeObject(hostToPartitionsMapping);
        out.writeInt(numberOfSlotsPerNode);
        out.writeBoolean(isSingleResultOptimisation);
        out.writeBoolean(isSortingEnabled);
        out.writeUTF(hadoopVersion);

        out.writeBoolean(inputSplitAssignment == null); //If no input splits were provided, record that
        if (inputSplitAssignment != null) {
            out.writeObject(splitType);
            boolean writable = Writable.class.isAssignableFrom(splitType);
            out.writeBoolean(writable);

            //We have to explicitly serialize split list, because its entries
            //might be non-Serilizable Writables.
            out.writeInt(inputSplits.size());
            for (INPUT_SPLIT_BASE_TYPE split : inputSplits) {
                if (writable) {
                    ((Writable) split).write(out);
                } else {
                    out.writeObject(split);
                }
            }

            out.writeObject(inputSplitAssignment);
        }
        out.close(); //Compressing stream factory streams will not close the inner stream
    }


    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream instream)
            throws IOException, ClassNotFoundException {

        ObjectInputStream in = new ObjectInputStream(CompressingStreamsFactory.getCompressedInputStream(instream));
        isOldApi = in.readBoolean();
        configurationClass = (Class<? extends Configuration>)in.readObject();
        configuration = ReflectionUtils.newInstance(configurationClass, null);
        jobId = isOldApi ? new org.apache.hadoop.mapred.JobID() : new org.apache.hadoop.mapreduce.JobID();
        configuration.readFields(in);
        configuration.setClassLoader(InvocationGridLocalCache.getClassLoader());
        jobId.readFields(in);
        jobParameter = in.readObject();
        appId = in.readInt();
        hadoopPartitionToSossRegionMapping = (int[]) in.readObject();
        hostToPartitionsMapping = (com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping) in.readObject();
        numberOfSlotsPerNode = in.readInt();
        isSingleResultOptimisation = in.readBoolean();
        isSortingEnabled = in.readBoolean();
        hadoopVersion = in.readUTF();


        if (!in.readBoolean())  //We have splits as part of the params
        {


            splitType = (Class<? extends INPUT_SPLIT_BASE_TYPE>) in.readObject();
            boolean writable = in.readBoolean();
            int numEntries = in.readInt();
            inputSplits = new ArrayList<INPUT_SPLIT_BASE_TYPE>(numEntries);

            for (int i = 0; i < numEntries; i++) {
                INPUT_SPLIT_BASE_TYPE split;
                if (writable) {
                    split = ReflectionUtils.newInstance(splitType, configuration);
                    ((Writable) split).readFields(in);
                } else {
                    split = (INPUT_SPLIT_BASE_TYPE) in.readObject();
                }
                inputSplits.add(split);
            }

            inputSplitAssignment = (Map<InetAddress, List<Integer>>)in.readObject();

        }
        parameterCacheByInvocationId.put(appId, this);

        in.close();  //Compressing stream factory streams will not close the inner stream
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public JobID getJobId() {
        return jobId;
    }

    public int[] getHadoopPartitionToSossRegionMapping() {
        return hadoopPartitionToSossRegionMapping;
    }

    HostToPartitionsMapping getHostToRegionsMapping() {
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

    Object getJobParameter() {
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
        return isOldApi;
    }


    @Override
    public String toString() {
        return "InvocationParameters{" +
                "configuration=" + configuration + dumpConfiguration(configuration)+
                ", configurationClass=" + configurationClass +
                ", jobId=" + jobId +
                ", appId=" + appId +
                ", hadoopPartitionToSossRegionMapping=" + Arrays.toString(hadoopPartitionToSossRegionMapping) +
                ", hostToPartitionsMapping=" + hostToPartitionsMapping +
                ", numberOfSlotsPerNode=" + numberOfSlotsPerNode +
                ", splitType=" + splitType +
                ", inputSplits=" + inputSplits +
                ", inputSplitAssignment=" + inputSplitAssignment +
                ", isSingleResultOptimisation=" + isSingleResultOptimisation +
                ", isSortingEnabled=" + isSortingEnabled +
                ", isOldApi=" + isOldApi +
                ", hadoopVersion='" + hadoopVersion + '\'' +
                ", jobParameter=" + jobParameter +
                '}';
    }

    public static String dumpConfiguration(Configuration conf)
      {
          StringBuilder builder = new StringBuilder();
          builder.append("{ ");
          for(Map.Entry<String,String> entry : conf)
          {
            builder.append("{");
            builder.append(entry.getKey());
            builder.append(",");
            builder.append(entry.getValue());
            builder.append("} ");
          }
          builder.append("}");
          return builder.toString();
      }

    /**
     * Retrieves the invocation parameters from the cache, where they should be saved by the mapper.
     *
     * @param invocationId invocation id (app id)
     * @return cached InvocationParameters
     * @throws IOException if invocation parameters cannot be found
     */
    public static InvocationParameters retrieveFromCache(int invocationId) throws IOException {
        InvocationParameters parameters = parameterCacheByInvocationId.get(invocationId);
        if (parameters == null) {
            throw new IOException("Cannot retrieve invocation parameters");
        }
        return parameters;
    }
}
