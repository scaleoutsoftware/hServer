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

import com.scaleoutsoftware.soss.client.*;
import com.scaleoutsoftware.soss.client.da.DataAccessor;
import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.client.util.SerializationMode;
import com.scaleoutsoftware.soss.client.pmi.MessagingHelper;
import com.scaleoutsoftware.soss.client.util.BitConverter;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopInvocationParameters;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopVersionSpecificCode;
import com.scaleoutsoftware.soss.hserver.interop.HServerConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.MAP_SPLITS_PER_CORE;

/**
 * A singleton scheduler which is used to run {@link com.scaleoutsoftware.soss.hserver.HServerJob} as a series of PMIs.
 */
public class JobScheduler {
    private static final Log LOG = LogFactory.getLog(JobScheduler.class);
    private static JobScheduler _instance = new JobScheduler();

    /**
     * Returns the single instance.
     *
     * @return scheduler instance
     */
    public static JobScheduler getInstance() {
        return _instance;
    }

    /**
     * Gets the locations for the split, handles both "mapred" and "mapreduce" splits.
     *
     * @param split split to get locations
     * @return array of split home locations
     */
    private String[] getSplitLocations(Object split) throws IOException, InterruptedException {
        if (split instanceof org.apache.hadoop.mapred.InputSplit) {
            return ((org.apache.hadoop.mapred.InputSplit) split).getLocations();
        } else if (split instanceof org.apache.hadoop.mapreduce.InputSplit) {
            return ((org.apache.hadoop.mapreduce.InputSplit) split).getLocations();
        } else {
            throw new IOException("Invalid split type:" + split);
        }
    }

    /**
     * Returns the SOSS host IPs which correspond to the locations for the given split.
     *
     * @param split                    split to locate
     * @param sossHostAdresses         list of available SOSS hosts
     * @param additionalSplitLocations additional locations for that split, can be null
     * @return list of split location, or empty list if none found
     */
    private List<InetAddress> getSossLocations(Object split, List<InetAddress> sossHostAdresses, String[] additionalSplitLocations) {
        List<InetAddress> splitLocations = new ArrayList<InetAddress>();

        try {
            //If GridSplit, just find and return its home location
            if (split instanceof GridSplit) {
                InetAddress location = ((GridSplit) split).getLocation();
                if (location != null && sossHostAdresses.contains(location)) {
                    splitLocations.add(location);
                    return splitLocations;
                }
            }

            //Parse locations in the split object
            String[] locations = getSplitLocations(split);
            if (locations != null) {
                for (String location : locations) {
                    try {
                        splitLocations.addAll(Arrays.asList(InetAddress.getAllByName(location)));
                    } catch (UnknownHostException e) {
                        //Do nothing, must be a bad location
                    }
                }
            }

            //Add additional locations, passed separate from the split
            if (additionalSplitLocations != null) {
                for (String location : additionalSplitLocations) {
                    try {
                        splitLocations.addAll(Arrays.asList(InetAddress.getAllByName(location)));
                    } catch (UnknownHostException e) {
                        //Do nothing, must be a bad location
                    }
                }
            }

            //Remove locations which are not SOSS locations
            Iterator<InetAddress> iterator = splitLocations.iterator();
            while (iterator.hasNext()) {
                if (!sossHostAdresses.contains(iterator.next())) {
                    iterator.remove();
                }
            }

        } catch (InterruptedException e) {
            //Do nothing, split will be assigned to the random location
        } catch (IOException e) {
            //Do nothing, split will be assigned to the random location
        }

        return splitLocations;
    }

    /**
     * Walks through the split list and assigns split to hosts.
     * The result of this method is the map in which host addresses are keys
     * and values are lists of split indexes.
     *
     * @param splitList      list of all splits in the job
     * @param hostAddresses  list of host addresses in the cluster
     * @param splitLocations additional home locations for splits, can be null
     * @return map of lists of splits for each host
     */
    private <INPUT_SPLIT_TYPE> Map<InetAddress, List<Integer>> assignSplitsToHost(List<INPUT_SPLIT_TYPE> splitList, List<InetAddress> hostAddresses, Map<Object, String[]> splitLocations) {
        Map<InetAddress, List<Integer>> splitToHostAddress = new HashMap<InetAddress, List<Integer>>();

        int maxSplitsPerHost = splitList.size() / hostAddresses.size();

        for (int splitIndex = 0; splitIndex < splitList.size(); splitIndex++) {
            INPUT_SPLIT_TYPE split = splitList.get(splitIndex);
            InetAddress hostToSendSplitTo = null;
            int minimumNumberOfSplitsAtLocation = Integer.MAX_VALUE;
            List<InetAddress> candidateLocations = getSossLocations(split, hostAddresses, splitLocations == null ? null : splitLocations.get(split));

            LOG.debug("Split " + split + ":" + candidateLocations+","+hostAddresses);

            //Go through all candidate locations, finding one with smaller number of splits
            for (InetAddress location : candidateLocations)   //Iterate through split candidate locations, trying to find the location with less splits
            {
                if (!hostAddresses.contains(location)) {
                    //The SOSS host address for the split is not contained in the list of the host addresses,
                    //that we received from the IG. Means no IG worker on that host.
                    LOG.warn("A split location " + location + " does not have a local worker available. Split " + split);
                } else {
                    int numberOfsplitsAtLocation = splitToHostAddress.containsKey(location) ? splitToHostAddress.get(location).size() : 0;

                    //1.The number of splits at that host is less than max allowed number of splits
                    //2.Host contains less splits that we have previously seen
                    if (numberOfsplitsAtLocation < maxSplitsPerHost && minimumNumberOfSplitsAtLocation > numberOfsplitsAtLocation) {
                        hostToSendSplitTo = location;
                        minimumNumberOfSplitsAtLocation = numberOfsplitsAtLocation;  //We will use it at the next iteration
                    }

                    if (numberOfsplitsAtLocation == 0) break; //We found location with no splits, so use it right away
                }
            }

            //We cannot send split to one of its home locations,so find the host with lowest number of splits assigned

            if (hostToSendSplitTo == null) {
                LOG.warn("Cannot assign split " + split + " to its home location. Candidate locations = " + candidateLocations);

                minimumNumberOfSplitsAtLocation = Integer.MAX_VALUE;

                for (InetAddress inetAddress : hostAddresses) {
                    int newNumberOfSplits = splitToHostAddress.containsKey(inetAddress) ? splitToHostAddress.get(inetAddress).size() : 0;
                    if (minimumNumberOfSplitsAtLocation > newNumberOfSplits) {
                        hostToSendSplitTo = inetAddress;
                        minimumNumberOfSplitsAtLocation = newNumberOfSplits;
                    }
                }
            }

            //Add split to the map
            if (!splitToHostAddress.containsKey(hostToSendSplitTo)) {
                splitToHostAddress.put(hostToSendSplitTo, new ArrayList<Integer>());
            }
            splitToHostAddress.get(hostToSendSplitTo).add(splitIndex);
        }
        return splitToHostAddress;
    }

    /**
     * Runs a single-result optimisation of the job (one PMI) and return the result.
     *
     * @param job  job to run
     * @param grid invocation grid to run job on
     * @return result object
     */
    @SuppressWarnings("unchecked")
    Object runOptimisation(HServerJob job, InvocationGrid grid) throws IOException, InterruptedException, ClassNotFoundException {
        long time = System.currentTimeMillis();
        CreateUserCredentials.run(grid);
        try {
            //Calculating the region layout
            com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostNameToPartition = com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping.getCurrent();
            List<InetAddress> hostAddresses = new ArrayList<InetAddress>(hostNameToPartition.getHosts());


            int numberOfSlotsPerNode = Math.max(grid != null ? grid.getMaxNumberOfCores() : Runtime.getRuntime().availableProcessors(), 1);

            //Set the number of splits to the number of cores
            if (GridInputFormat.class.isAssignableFrom(job.getInputFormatClass())) {
                int numberOfSplits = HServerParameters.getSetting(MAP_SPLITS_PER_CORE, job.getConfiguration()) * hostAddresses.size() * numberOfSlotsPerNode;
                GridInputFormat.setSuggestedNumberOfSplits(job, Math.min(numberOfSplits, HServerConstants.MAX_MAP_REDUCE_TASKS));
            }

            //Generating split to hostname map
            InputFormat inputFormat = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
            List<InputSplit> splitList = inputFormat.getSplits(job);
            Map<InetAddress, List<Integer>> splitToHostAddress = assignSplitsToHost(splitList, hostAddresses, null);

            //Generating invocation parameters
            String hadoopVersion = VersionInfo.getVersion();
            Class<? extends InputSplit> splitType = splitList.size() > 0 ? splitList.get(0).getClass() : null;

            HadoopInvocationParameters hadoopParameters = new HadoopInvocationParameters(job.getConfiguration(), job.getJobID(), false);
            HServerInvocationParameters parameters = new HServerInvocationParameters(
                    hadoopParameters,
                    job.getAppId(),
                    new int[0],
                    hostNameToPartition,
                    numberOfSlotsPerNode,
                    splitType,
                    splitList,
                    splitToHostAddress,
                    true,
                    false,
                    hadoopVersion,
                    job.getJobParameter(),
                    SerializationMode.DEFAULT);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Splits created:\n");
            for (InetAddress address : splitToHostAddress.keySet()) {
                stringBuilder.append("Host ");
                stringBuilder.append(address);
                stringBuilder.append(" has ");
                stringBuilder.append(splitToHostAddress.get(address).size());
                stringBuilder.append(" splits.\n");
            }
            System.out.println(stringBuilder.toString());
            System.out.println("Job initialization completed in " + (System.currentTimeMillis() - time) + " ms.");


            InvokeResult<MapperResult> invokeResult = MessagingHelper.invoke(grid, RunMapper.MapperInvokable.class, parameters, TimeSpan.INFINITE_TIMEOUT.getSeconds());

            if (invokeResult.getErrors() != null && invokeResult.getErrors().size() > 0) {
                throw new IOException("Map invocation failed.", invokeResult.getErrors().get(0));
            }

            MapperResult result = invokeResult.getResult();

            if (result == null || invokeResult.getNumFailed() != 0) {
                throw new IOException("Mapper invocation failed");
            }

            if (result.getNumberOfSplitsProcessed() != splitList.size()) {
                throw new IOException("Number of splits does not match the number of invocations. Nsplits = " + splitList.size() + ", Ninvokes =" + result.getNumberOfSplitsProcessed());
            }

            Map<String, Long> processingTimes = result.getProcessingTimes();

            for (String host : processingTimes.keySet()) {
                System.out.println("Host " + host + ": invoke done in " + processingTimes.get(host) + " ms.");
            }

            return result.getResult();
        } catch (StateServerException e) {
            throw new IOException("ScaleOut hServer access error.", e);
        }
    }

    /**
     * Runs the map-reduce job on ScaleOut hServer.
     *
     * @param job  the job to run
     * @param grid invocation grid to run the job
     */
    @SuppressWarnings("unchecked")
    void runJob(HServerJob job, InvocationGrid grid) throws IOException, InterruptedException, ClassNotFoundException {
        //Initialize user credential in advance
        long time = System.currentTimeMillis();
        CreateUserCredentials.run(grid);
        String hadoopVersion = VersionInfo.getVersion();

        try {
            //Check output specs before running the job
            OutputFormat outputFormat = ReflectionUtils.newInstance(job.getOutputFormatClass(), job.getConfiguration());
            outputFormat.checkOutputSpecs(job);

            org.apache.hadoop.mapreduce.OutputCommitter outputCommitter = createOutputCommitter(true, job.getJobID(), job.getConfiguration());

            //clear all temporary objects
            DataAccessor.clearObjects(job.getAppId());

            //Calculating the partition layout
            com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostNameToPartition = com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping.getCurrent();
            List<InetAddress> hostAddresses = new ArrayList<InetAddress>(hostNameToPartition.getHosts());

            //Generating mapping of Hadoop partitions to SOSS Regions, so they are equally distributed across hosts
            int numHosts = hostAddresses.size();
            int numberOfSlotsPerNode = Math.max(grid != null ? grid.getMaxNumberOfCores() : Runtime.getRuntime().availableProcessors(), 1);

            //Set the number of splits to the number of cores
            if (GridInputFormat.class.isAssignableFrom(job.getInputFormatClass())) {
                int numberOfSplits = HServerParameters.getSetting(MAP_SPLITS_PER_CORE, job.getConfiguration()) * numHosts * numberOfSlotsPerNode;
                GridInputFormat.setSuggestedNumberOfSplits(job, Math.min(numberOfSplits, HServerConstants.MAX_MAP_REDUCE_TASKS));
            }

            //Generating split to hostname map
            InputFormat inputFormat = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
            List<InputSplit> splitList = inputFormat.getSplits(job);
            Map<InetAddress, List<Integer>> splitToHostAddress = assignSplitsToHost(splitList, hostAddresses, null);


            //Choose the optimal number of reducers for GridOutputFormat
            if (GridOutputFormat.class.isAssignableFrom(job.getOutputFormatClass())) {
                job.setNumReduceTasks(numHosts * numberOfSlotsPerNode);
                job.setSortEnabled(false);
            }


            int[] partitionMapping = hostNameToPartition.generateEvenItemDistribution(job.getNumReduceTasks());

            //Generating invocation parameters
            Class<? extends InputSplit> splitType = splitList.size() > 0 ? splitList.get(0).getClass() : null;

            HadoopInvocationParameters hadoopParameters = new HadoopInvocationParameters(job.getConfiguration(), job.getJobID(), false);
            HServerInvocationParameters parameters = new HServerInvocationParameters(
                    hadoopParameters,
                    job.getAppId(),
                    partitionMapping,
                    hostNameToPartition,
                    numberOfSlotsPerNode,
                    splitType,
                    splitList,
                    splitToHostAddress,
                    false,
                    job.getSortEnabled(),
                    hadoopVersion,
                    job.getJobParameter(),
                    SerializationMode.DEFAULT);


            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Splits created:\n");
            for (InetAddress address : splitToHostAddress.keySet()) {
                stringBuilder.append("Host ");
                stringBuilder.append(address);
                stringBuilder.append(" has ");
                stringBuilder.append(splitToHostAddress.get(address).size());
                stringBuilder.append(" splits.\n");
            }
            System.out.println(stringBuilder.toString());

            System.out.println("Job initialization completed in " + (System.currentTimeMillis() - time) + " ms.");
            time = System.currentTimeMillis();

            InvokeResult<MapperResult> mapInvokeResult = MessagingHelper.invoke(grid, RunMapper.MapperInvokable.class, parameters, TimeSpan.INFINITE_TIMEOUT.getSeconds());

            if (mapInvokeResult.getErrors() != null && mapInvokeResult.getErrors().size() > 0) {
                throw new IOException("Map invocation failed.", mapInvokeResult.getErrors().get(0));
            }

            System.out.println("Map invocation done in " + (System.currentTimeMillis() - time) + " ms.");
            time = System.currentTimeMillis();

            MapperResult resultObject = mapInvokeResult.getResult();

            if (resultObject == null || mapInvokeResult.getNumFailed() != 0) {
                throw new IOException("Mapper invocation failed. Num failed = " + mapInvokeResult.getNumFailed());
            }

            if (resultObject.getNumberOfSplitsProcessed() != splitList.size()) {
                throw new IOException("Number of splits does not match the number of invocations. Nsplits = " + splitList.size() + ", Ninvokes =" + resultObject.getNumberOfSplitsProcessed());
            }

            if (partitionMapping.length > 0) {
                //Running the reduce step
                InvokeResult<Integer> reduceInvokeResult = MessagingHelper.invoke(grid, ReduceInvokable.class, job.getAppId(), TimeSpan.INFINITE_TIMEOUT.getSeconds());

                System.out.println("Reduce invocation done in " + (System.currentTimeMillis() - time) + " ms.");

                DataAccessor.clearObjects(job.getAppId()); //clear all temporary objects

                if (reduceInvokeResult.getErrors() != null && reduceInvokeResult.getErrors().size() > 0) {
                    throw new IOException("Reduce invocation failed.", reduceInvokeResult.getErrors().get(0));
                }
                if (reduceInvokeResult.getNumFailed() != 0) {
                    throw new IOException("Reduce invocation failed.");
                }
                if (reduceInvokeResult.getResult() != partitionMapping.length) {
                    throw new IOException("Not all partitions were reduced. Expected = " + partitionMapping.length + " Actual = " + reduceInvokeResult.getResult());
                }
            }
            outputCommitter.commitJob(job);
        } catch (StateServerException e) {
            throw new IOException("ScaleOut hServer access error.", e);
        }

    }

    /**
     * Runs the map-reduce job on ScaleOut hServer.
     *
     * @param jobID          the id of the job
     * @param jobConf        the job to run
     * @param isNewApi       if the job uses the new MapReduce APIs
     * @param splitType      the type of the split
     * @param inputSplits    the list of input splits
     * @param splitLocations the locations of the splits
     * @param grid           the invocation grid to run the job
     * @throws IOException            if errors occurred during the job
     * @throws InterruptedException   if the processing thread is interrupted
     * @throws ClassNotFoundException if the invocation grid does not contain the dependency class
     */
    @SuppressWarnings("unchecked")
    public void runPredefinedJob(JobID jobID, JobConf jobConf, boolean isNewApi, Class splitType, List<?> inputSplits, Map<Object, String[]> splitLocations, InvocationGrid grid) throws IOException, InterruptedException, ClassNotFoundException {

        //Initialize user credential in advance
        long time = System.currentTimeMillis();
        CreateUserCredentials.run(grid);
        String hadoopVersion = VersionInfo.getVersion();

        int appID = 0xFFFFFFF & BitConverter.hashStringOneInt(jobID.toString());

        try {

            org.apache.hadoop.mapreduce.OutputCommitter outputCommitter = createOutputCommitter(isNewApi, jobID, jobConf);

            HadoopVersionSpecificCode hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(hadoopVersion, jobConf);

            org.apache.hadoop.mapred.JobContext jobContext = hadoopVersionSpecificCode.createJobContext(jobConf, jobID);
            outputCommitter.setupJob(jobContext);

            //clear all temporary objects
            DataAccessor.clearObjects(appID);

            //Calculating the partition layout
            com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostNameToPartition = com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping.getCurrent();
            List<InetAddress> hostAddresses = new ArrayList<InetAddress>(hostNameToPartition.getHosts());

            //Generating mapping of Hadoop partitions to SOSS partitions, so they are equally distributed across hosts
            int numHosts = hostAddresses.size();
            int numberOfSlotsPerNode = Math.max(grid != null ? grid.getMaxNumberOfCores() : Runtime.getRuntime().availableProcessors(), 1);

            //Generating split to hostname map
            Map<InetAddress, List<Integer>> splitToHostAddress = assignSplitsToHost(inputSplits, hostAddresses, splitLocations);

            int[] partitionMapping = hostNameToPartition.generateEvenItemDistribution(jobConf.getNumReduceTasks());

            HadoopInvocationParameters hadoopParameters = new HadoopInvocationParameters(jobConf, jobID, !isNewApi);
            HServerInvocationParameters parameters = new HServerInvocationParameters(
                    hadoopParameters,
                    appID,
                    partitionMapping,
                    hostNameToPartition,
                    numberOfSlotsPerNode,
                    splitType,
                    inputSplits,
                    splitToHostAddress,
                    false,
                    HServerParameters.getBooleanSetting(HServerParameters.SORT_KEYS, jobConf),
                    hadoopVersion,
                    null,
                    SerializationMode.DEFAULT);


            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Splits created:\n");
            for (InetAddress address : splitToHostAddress.keySet()) {
                stringBuilder.append("Host ");
                stringBuilder.append(address);
                stringBuilder.append(" has ");
                stringBuilder.append(splitToHostAddress.get(address).size());
                stringBuilder.append(" splits.\n");
            }
            System.out.println(stringBuilder.toString());

            System.out.println("Job initialization completed in " + (System.currentTimeMillis() - time) + " ms.");

            time = System.currentTimeMillis();

            InvokeResult<MapperResult> mapInvokeResult = MessagingHelper.invoke(grid, RunMapper.MapperInvokable.class, parameters, TimeSpan.INFINITE_TIMEOUT.getSeconds());

            if (mapInvokeResult.getErrors() != null && mapInvokeResult.getErrors().size() > 0) {
                throw new IOException("Map invocation failed.", mapInvokeResult.getErrors().get(0));
            }

            System.out.println("Map invocation done in " + (System.currentTimeMillis() - time) + " ms.");
            time = System.currentTimeMillis();

            MapperResult resultObject = mapInvokeResult.getResult();

            if (resultObject == null || mapInvokeResult.getNumFailed() != 0) {
                throw new IOException("Mapper invocation failed. Num failed = " + mapInvokeResult.getNumFailed());
            }

            if (resultObject.getNumberOfSplitsProcessed() != inputSplits.size()) {
                throw new IOException("Number of splits does not match the number of invocations. Nsplits = " + inputSplits.size() + ", Ninvokes =" + resultObject.getNumberOfSplitsProcessed());
            }

            if (partitionMapping.length > 0) {
                //Running the reduce step
                InvokeResult<Integer> reduceInvokeResult = MessagingHelper.invoke(grid, ReduceInvokable.class, appID, TimeSpan.INFINITE_TIMEOUT.getSeconds());

                System.out.println("Reduce invocation done in " + (System.currentTimeMillis() - time) + " ms.");

                DataAccessor.clearObjects(appID); //clear all temporary objects

                if (reduceInvokeResult.getErrors() != null && reduceInvokeResult.getErrors().size() > 0) {
                    throw new IOException("Reduce invocation failed.", reduceInvokeResult.getErrors().get(0));
                }
                if (reduceInvokeResult.getNumFailed() != 0) {
                    throw new IOException("Reduce invocation failed.");
                }
                if (reduceInvokeResult.getResult() != partitionMapping.length) {
                    throw new IOException("Not all partitions were reduced. Expected = " + partitionMapping.length + " Actual = " + reduceInvokeResult.getResult());
                }
            }
            outputCommitter.commitJob(jobContext);
        } catch (StateServerException e) {
            throw new IOException("ScaleOut hServer access error.", e);
        }

    }

    //Taken from LocalJobRunner.java in Apache Hadoop 2.2.0
    private org.apache.hadoop.mapreduce.OutputCommitter
    createOutputCommitter(boolean newApiCommitter, JobID jobId, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        org.apache.hadoop.mapreduce.OutputCommitter committer = null;

        LOG.info("OutputCommitter set in config "
                + conf.get("mapred.output.committer.class"));

        if (newApiCommitter) {
            HadoopVersionSpecificCode hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(VersionInfo.getVersion(), conf);
            org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID = hadoopVersionSpecificCode.createTaskAttemptId(jobId, true, 0);
            org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = hadoopVersionSpecificCode.createTaskAttemptContext(conf, taskAttemptID);
            OutputFormat outputFormat =
                    ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), conf);
            committer = outputFormat.getOutputCommitter(taskContext);
        } else {
            committer = ReflectionUtils.newInstance(conf.getClass(
                    "mapred.output.committer.class", FileOutputCommitter.class,
                    org.apache.hadoop.mapred.OutputCommitter.class), conf);
        }
        LOG.info("OutputCommitter is " + committer.getClass().getName());
        return committer;
    }

    /**
     * Runs the map-reduce job on ScaleOut hServer.*
     *
     * @param job          the job to run
     * @param jobId        the id of the job
     * @param sortEnabled  if key sorting is enabled
     * @param jobParameter user defined parameter object for the job
     * @param grid         the invocation grid to run the job
     * @throws IOException            if errors occurred during the job
     * @throws InterruptedException   if the processing thread is interrupted
     * @throws ClassNotFoundException if the invocation grid does not contain the dependency class
     */
    @SuppressWarnings("unchecked")
    public void runOldApiJob(JobConf job, org.apache.hadoop.mapred.JobID jobId, boolean sortEnabled, Object jobParameter, InvocationGrid grid) throws IOException, InterruptedException, ClassNotFoundException {
        //Initialize user credential in advance
        int jobAppId = 0xFFFFFFF & BitConverter.hashStringOneInt(jobId.toString());
        String hadoopVersion = VersionInfo.getVersion();
        long time = System.currentTimeMillis();
        CreateUserCredentials.run(grid);

        try {
            //Check output specs before running the job
            job.getOutputFormat().checkOutputSpecs(FileSystem.get(job), job);

            JobContext jContext = HadoopVersionSpecificCode.getInstance(hadoopVersion, job).createJobContext(job, jobId);

            org.apache.hadoop.mapred.OutputCommitter outputCommitter = job.getOutputCommitter();
            outputCommitter.setupJob(jContext);

            //clear all temporary objects
            DataAccessor.clearObjects(jobAppId);

            //Calculating the partition layout
            com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping hostNameToPartition = com.scaleoutsoftware.soss.client.util.HostToPartitionsMapping.getCurrent();
            List<InetAddress> hostAddresses = new ArrayList<InetAddress>(hostNameToPartition.getHosts());

            //Generating mapping of Hadoop partitions to SOSS partitions, so they are equally distributed across hosts
            int numHosts = hostAddresses.size();
            int numberOfSlotsPerNode = Math.max(grid != null ? grid.getMaxNumberOfCores() : Runtime.getRuntime().availableProcessors(), 1);

            //Set the number of splits to the number of cores
            if (NamedMapInputFormatMapred.class.isAssignableFrom(job.getInputFormat().getClass())) {
                int numberOfSplits = HServerParameters.getSetting(MAP_SPLITS_PER_CORE, job) * numHosts * numberOfSlotsPerNode;
                job.setNumMapTasks(Math.min(numberOfSplits, HServerConstants.MAX_MAP_REDUCE_TASKS));
            }

            //Generating split to hostname map
            org.apache.hadoop.mapred.InputFormat inputFormat = job.getInputFormat();
            List<org.apache.hadoop.mapred.InputSplit> splitList = Arrays.asList(inputFormat.getSplits(job, job.getNumMapTasks()));
            Map<InetAddress, List<Integer>> splitToHostAddress = assignSplitsToHost(splitList, hostAddresses, null);

            //Choose the optimal number of reducers for GridOutputFormat
            if (job.getOutputFormat() instanceof NamedMapOutputFormatMapred) {
                job.setNumReduceTasks(numHosts * numberOfSlotsPerNode);
                sortEnabled = false;
            }

            int[] partitionMapping = hostNameToPartition.generateEvenItemDistribution(job.getNumReduceTasks());

            //Generating invocation parameters
            Class<? extends org.apache.hadoop.mapred.InputSplit> splitType = splitList.size() > 0 ? splitList.get(0).getClass() : null;

            HadoopInvocationParameters hadoopParameters = new HadoopInvocationParameters(job, jobId, true);

            HServerInvocationParameters<org.apache.hadoop.mapred.InputSplit> parameters = new HServerInvocationParameters<org.apache.hadoop.mapred.InputSplit>
                    (hadoopParameters,
                    jobAppId,
                    partitionMapping,
                    hostNameToPartition,
                    numberOfSlotsPerNode,
                    splitType,
                    splitList,
                    splitToHostAddress,
                    false,
                    sortEnabled,
                    hadoopVersion,
                    jobParameter,
                    SerializationMode.DEFAULT);


            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Splits created:\n");
            for (InetAddress address : splitToHostAddress.keySet()) {
                stringBuilder.append("Host ");
                stringBuilder.append(address);
                stringBuilder.append(" has ");
                stringBuilder.append(splitToHostAddress.get(address).size());
                stringBuilder.append(" splits.\n");
            }
            System.out.println(stringBuilder.toString());

            System.out.println("Job initialization completed in " + (System.currentTimeMillis() - time) + " ms.");
            time = System.currentTimeMillis();

            InvokeResult<MapperResult> mapInvokeResult = MessagingHelper.invoke(grid, RunMapper.MapperInvokable.class, parameters, TimeSpan.INFINITE_TIMEOUT.getSeconds());

            if (mapInvokeResult.getErrors() != null && mapInvokeResult.getErrors().size() > 0) {
                throw new IOException("Map invocation failed.", mapInvokeResult.getErrors().get(0));
            }

            System.out.println("Map invocation done in " + (System.currentTimeMillis() - time) + " ms.");
            time = System.currentTimeMillis();

            MapperResult resultObject = mapInvokeResult.getResult();

            if (resultObject == null || mapInvokeResult.getNumFailed() != 0) {
                throw new IOException("Mapper invocation failed. Num failed = " + mapInvokeResult.getNumFailed());
            }

            if (resultObject.getNumberOfSplitsProcessed() != splitList.size()) {
                throw new IOException("Number of splits does not match the number of invocations. Nsplits = " + splitList.size() + ", Ninvokes =" + resultObject.getNumberOfSplitsProcessed());
            }

            if (partitionMapping.length > 0) {
                //Running the reduce step
                InvokeResult<Integer> reduceInvokeResult = MessagingHelper.invoke(grid, ReduceInvokable.class, jobAppId, TimeSpan.INFINITE_TIMEOUT.getSeconds());

                System.out.println("Reduce invocation done in " + (System.currentTimeMillis() - time) + " ms.");

                DataAccessor.clearObjects(jobAppId); //clear all temporary objects

                if (reduceInvokeResult.getErrors() != null && reduceInvokeResult.getErrors().size() > 0) {
                    throw new IOException("Reduce invocation failed.", reduceInvokeResult.getErrors().get(0));
                }
                if (reduceInvokeResult.getNumFailed() != 0) {
                    throw new IOException("Reduce invocation failed.");
                }
                if (reduceInvokeResult.getResult() != partitionMapping.length) {
                    throw new IOException("Not all partitions were reduced. Expected = " + partitionMapping.length + " Actual = " + reduceInvokeResult.getResult());
                }
            }
            outputCommitter.commitJob(jContext);
        } catch (StateServerException e) {
            throw new IOException("ScaleOut hServer access error.", e);
        }

    }


    /**
     * Creates user credentials, saving time during the invocation.
     */
    public static class CreateUserCredentials implements Invokable<Integer, String, Integer> {
        public static void run(InvocationGrid grid) {
            try {
                MessagingHelper.invoke(grid, CreateUserCredentials.class, "", 0).getNumSuccessful();
            } catch (Exception e) {
                //Do nothing, this is an optimization
            }
        }

        @Override
        public Integer eval(Integer integer, String user, EvalArgs<Integer> integerEvalArgs) throws InvokeException, InterruptedException {
            try {
                UserGroupInformation.getCurrentUser();
            } catch (Throwable t) {
                //Do nothing, this is an optimization
            }
            return 0;
        }

        @Override
        public Integer merge(Integer integer, Integer integer2) throws InvokeException, InterruptedException {
            return 0;
        }
    }

}
