package com.scaleoutsoftware.soss.hserver;


import com.scaleoutsoftware.soss.client.util.SerializationMode;
import com.scaleoutsoftware.soss.client.pmi.InvocationGridLocalCache;
import com.scaleoutsoftware.soss.client.pmi.InvocationWorker;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopInvocationParameters;
import com.scaleoutsoftware.soss.hserver.interop.HServerConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

public class HServerInvocationParameters<INPUT_SPLIT_BASE_TYPE> extends InvocationParameters<INPUT_SPLIT_BASE_TYPE> implements Serializable {
    private static final Logger _logger = LoggerFactory.getLogger(HServerInvocationParameters.class);
    protected static final long serialVersionUID = 4L;
    //LRU map for the invocation parameters. The invocation parameters are put in this map
    //by at the map invocation step, to be retrieved at the reduce step.
    private final static Map<Integer, HServerInvocationParameters> parameterCacheByInvocationId = Collections.synchronizedMap(new LinkedHashMap<Integer, HServerInvocationParameters>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry eldest) {
            return super.size() > HServerConstants.MAX_SIMULTANEOUS_JOBS;
        }
    });

    public HServerInvocationParameters( HadoopInvocationParameters hadoopInvocationParameters,
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
        super(hadoopInvocationParameters, appId, hadoopPartitionToSossPartitionMapping, hostToPartitionsMapping, numberOfSlotsPerNode, splitType, inputSplits, inputSplitAssignment, isSingleResultOptimisation, isSortingEnabled, hadoopVersion, jobParameter, serializationMode);

    }

    private void writeObject(java.io.ObjectOutputStream outstream) throws IOException {
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(CompressingStreamsFactory.getCompressedOutputStream(outstream));
        hadoopInvocationParameters.serialize(objectOutputStream);
        objectOutputStream.writeObject(jobParameter);
        objectOutputStream.writeInt(appId);
        objectOutputStream.writeObject(hadoopPartitionToSossPartitionMapping);
        objectOutputStream.writeObject(hostToPartitionsMapping);
        objectOutputStream.writeInt(numberOfSlotsPerNode);
        objectOutputStream.writeBoolean(isSingleResultOptimisation);
        objectOutputStream.writeBoolean(isSortingEnabled);
        objectOutputStream.writeUTF(hadoopVersion);
        objectOutputStream.writeInt(serializationMode.ordinal());

        objectOutputStream.writeBoolean(inputSplitAssignment == null); //If no input splits were provided, record that
        if (inputSplitAssignment != null) {
            objectOutputStream.writeObject(splitType);
            boolean writable = Writable.class.isAssignableFrom(splitType);
            objectOutputStream.writeBoolean(writable);

            //We have to explicitly serialize split list, because its entries
            //might be non-Serilizable Writables.
            objectOutputStream.writeInt(inputSplits.size());
            for (INPUT_SPLIT_BASE_TYPE split : inputSplits) {
                if (writable) {
                    ((Writable) split).write(objectOutputStream);
                } else {
                    objectOutputStream.writeObject(split);
                }
            }

            objectOutputStream.writeObject(inputSplitAssignment);
        }
        objectOutputStream.close(); //Compressing stream factory streams will not close the inner stream
    }


    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream instream) throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(CompressingStreamsFactory.getCompressedInputStream(instream))
        {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                try {
                    return super.resolveClass(desc);
                } catch (ClassNotFoundException e) {
                    //Continue to IG loader
                }
                if (InvocationWorker.isInvocationWorker()) {
                    try {
                        //This is to make sure that we use  the right class
                        return InvocationGridLocalCache.getClassLoader().loadClass(desc.getName());
                    } catch (Exception e) {
                        //Do nothing, default method will do the job.
                    }
                }
                throw new ClassNotFoundException(desc.getName());
            }
        };

        hadoopInvocationParameters = new HadoopInvocationParameters(in);
        jobParameter = in.readObject();
        appId = in.readInt();
        hadoopPartitionToSossPartitionMapping = (int[]) in.readObject();
        hostToPartitionsMapping = (com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping) in.readObject();
        numberOfSlotsPerNode = in.readInt();
        isSingleResultOptimisation = in.readBoolean();
        isSortingEnabled = in.readBoolean();
        hadoopVersion = in.readUTF();
        serializationMode = SerializationMode.values()[in.readInt()];


        if (!in.readBoolean())  //We have splits as part of the params
        {
            splitType = (Class<? extends INPUT_SPLIT_BASE_TYPE>) in.readObject();
            boolean writable = in.readBoolean();
            int numEntries = in.readInt();
            inputSplits = new ArrayList<INPUT_SPLIT_BASE_TYPE>(numEntries);

            for (int i = 0; i < numEntries; i++) {
                INPUT_SPLIT_BASE_TYPE split;
                if (writable) {
                    split = ReflectionUtils.newInstance(splitType, null);
                    ((Writable) split).readFields(in);
                } else {
                    split = (INPUT_SPLIT_BASE_TYPE) in.readObject();
                }
                inputSplits.add(split);
            }

            inputSplitAssignment = (Map<InetAddress, List<Integer>>) in.readObject();

        }
        parameterCacheByInvocationId.put(appId, this);

        in.close();  //Compressing stream factory streams will not close the inner stream
    }

    /**
     * Retrieves the invocation parameters from the cache, where they should be saved by the mapper.
     *
     * @param invocationId invocation id (app id)
     * @return cached InvocationParameters
     * @throws IOException if invocation parameters cannot be found
     */
    public static HServerInvocationParameters retrieveFromCache(int invocationId) throws IOException {
        HServerInvocationParameters parameters = parameterCacheByInvocationId.get(invocationId);
        if (parameters == null) {
            throw new IOException("Cannot retrieve invocation parameters");
        }
        return parameters;
    }
}
