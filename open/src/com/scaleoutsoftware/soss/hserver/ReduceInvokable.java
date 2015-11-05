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

import com.scaleoutsoftware.soss.client.EvalArgs;
import com.scaleoutsoftware.soss.client.Invokable;
import com.scaleoutsoftware.soss.client.InvokeException;
import com.scaleoutsoftware.soss.client.pmi.InvocationWorker;
import com.scaleoutsoftware.soss.client.util.NetUtils;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopVersionSpecificCode;
import com.scaleoutsoftware.soss.hserver.hadoop.ReducerWrapper;
import com.scaleoutsoftware.soss.hserver.hadoop.ReducerWrapperMapred;
import com.scaleoutsoftware.soss.hserver.hadoop.ReducerWrapperMapreduce;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 * Invokable for reduce stage
 * We do "invoke once", and then kick off reduce tasks for
 * the local partitions on the task thread pool
 */
public class ReduceInvokable implements Invokable<Integer, Integer, Integer> {
    private final static Set<InetAddress> localInterfaces = NetUtils.getLocalInterfaces();

    /**
     * Runs the reducer for a given partition.
     */
    protected static class ReduceTask implements Callable {
        protected int appId;
        protected int hadoopPartition;
        protected int sossPartition;
        protected Semaphore maxParallelTasks;


        protected ReduceTask(int appId, int hadoopPartition, int sossPartition, Semaphore maxParallelTasks) {
            this.appId = appId;
            this.hadoopPartition = hadoopPartition;
            this.sossPartition = sossPartition;
            this.maxParallelTasks = maxParallelTasks;
        }

        @Override
        public Object call() throws Exception {
            HServerInvocationParameters invocationParameters = HServerInvocationParameters.retrieveFromCache(appId);

            boolean sort = invocationParameters.isSortingEnabled();

            ReducerWrapper reducer;



            if (invocationParameters.isOldApi()) {
                reducer = new ReducerWrapperMapred(invocationParameters, hadoopPartition, appId, sossPartition, sort);
            } else {
                reducer = new ReducerWrapperMapreduce(invocationParameters, hadoopPartition, appId, sossPartition, sort);
            }

            maxParallelTasks.acquire();
            reducer.runReducer();
            maxParallelTasks.release();

            return null;
        }
    }

    @Override
    public Integer eval(Integer partition, final Integer appId, EvalArgs<Integer> serializableInputSplitEvalArgs) throws InvokeException, InterruptedException {
        HServerInvocationParameters invocationParameters;
        try {
            invocationParameters = HServerInvocationParameters.retrieveFromCache(appId);
        } catch (IOException e) {
            throw new InvokeException("Cannot retrieve parameters for reduce.", e);
        }

        try {
            Set<Integer> localSossRegions = invocationParameters.getHostToRegionsMapping().getLocalPartitions();

            if (localSossRegions == null) {
                //We have no partitions on that host (possibly a newly joined host)
                return 0;
            }

            int[] regionMapping = invocationParameters.getHadoopPartitionToSossRegionMapping();

            List<Integer> localHadoopPartitions = new ArrayList<Integer>();

            //Determine list of Hadoop partitions for that IP.
            //This list will have to be further divided between workers(if we have more than one).
            for (int i = 0; i < regionMapping.length; i++) {
                if (localSossRegions.contains(regionMapping[i])) {
                    localHadoopPartitions.add(i);
                }
            }

            //Handle the workload sharing between multiple JVMs
            if (InvocationWorker.getNumberOfWorkers() > 1) {
                Collections.sort(localHadoopPartitions); //All workers have partitions in the same order
                int listSize = localHadoopPartitions.size();
                int partitionsPerWorker = Math.max(1, listSize / InvocationWorker.getNumberOfWorkers() + 1);
                int startIndex = partitionsPerWorker * InvocationWorker.getIgWorkerIndex();

                if (startIndex < listSize) {
                    int stopIndex = Math.min(listSize, startIndex + partitionsPerWorker);
                    localHadoopPartitions = localHadoopPartitions.subList(startIndex, stopIndex);
                } else {
                    localHadoopPartitions = Collections.EMPTY_LIST;
                }
            }

            Set<Future> reduceTasks = new HashSet<Future>();

            // If there is a cap on maximum number of slots, apply it
            int maxSlots = HServerParameters.getSetting(HServerParameters.MAX_SLOTS, (Configuration)invocationParameters.getConfiguration());
            Semaphore maxParallelTasks = new Semaphore(maxSlots > 0 ? maxSlots : Integer.MAX_VALUE);

            for (Integer hadoopPartition : localHadoopPartitions) {
                //We have local Hadoop partition hadoopPartition, which corresponds to a SOSS region,
                //do reduce on that partition
                reduceTasks.add(RunMapper.taskExecutor.submit(new ReduceTask(appId, hadoopPartition, regionMapping[hadoopPartition],maxParallelTasks)));
            }

            for (Future reduceTask : reduceTasks) {
                try {
                    reduceTask.get();
                } catch (Exception e) {
                    throw new InvokeException("Reduce task failed", e);
                }
            }
            return reduceTasks.size();
        } finally {
            //Do the job cleanup on that host
            try {
                HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), (Configuration)invocationParameters.getConfiguration()).onJobDone(invocationParameters);
            } catch (IOException e) {
                throw new InvokeException("Cleanup failed.", e);
            }
        }
    }

    @Override
    public Integer merge(Integer integer, Integer integer2) throws InvokeException, InterruptedException {
        return integer + integer2;
    }
}
