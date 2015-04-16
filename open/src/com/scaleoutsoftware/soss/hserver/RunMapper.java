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
import com.scaleoutsoftware.soss.client.util.MergeTree;
import com.scaleoutsoftware.soss.client.util.NetUtils;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopVersionSpecificCode;
import com.scaleoutsoftware.soss.hserver.hadoop.MapperWrapper;
import com.scaleoutsoftware.soss.hserver.hadoop.MapperWrapperMapred;
import com.scaleoutsoftware.soss.hserver.hadoop.MapperWrapperMapreduce;
import com.scaleoutsoftware.soss.hserver.interop.DataGridWriterParameters;
import com.scaleoutsoftware.soss.hserver.interop.HServerConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.*;

class RunMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> {
    private static Log _logger = LogFactory.getLog(RunMapper.class);

    private final Configuration configuration;
    private final ArrayBlockingQueue<Integer> splitIndexesForThisHost;
    private int numberOfWorkers;
    private final int numberOfSplits;
    private final int invocationId; //appId identifying the job
    private final boolean isSingleResultOptimisation;
    private final RunHadoopMapContext<OUTKEY, OUTVALUE> runMapContext;
    private final MapperWrapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapperWrapper;
    private final List<?> inputSplitList;
    private final InvocationParameters invocationParameters;


    //LRU map for the combiners, so they will not hang in memory indefinitely.
    //The combiner are cached at he eval() step, so they can be retrieved
    //during the merge() step, at which there is no InvocationParameters available to
    //construct the combiner.
    final static Map<Integer, SingleKeyMapOutputAccumulator> combinerCacheByInvocationId = Collections.synchronizedMap(new LinkedHashMap<Integer, SingleKeyMapOutputAccumulator>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry eldest) {
            return super.size() > HServerConstants.MAX_SIMULTANEOUS_JOBS;
        }
    });


    static final ExecutorService taskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            //Make threads daemons. This may be excessive, because
            //the executor will be shut down anyway, but wont hurt.
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setDaemon(true);
            thread.setName("Map/Reduce executor: " + thread.getName());
            return thread;
        }
    });


    @SuppressWarnings("unchecked")
    RunMapper(InvocationParameters invocationParameters) throws IOException, ClassNotFoundException, NoSuchMethodException {
        _logger.debug("Starting mapper. Parameters: " + invocationParameters);

        this.invocationParameters = invocationParameters;

        if (invocationParameters.isOldApi()) {
            mapperWrapper = new MapperWrapperMapred<INKEY, INVALUE, OUTKEY, OUTVALUE>(invocationParameters);
        } else {
            mapperWrapper = new MapperWrapperMapreduce<INKEY, INVALUE, OUTKEY, OUTVALUE>(invocationParameters);
        }

        configuration = invocationParameters.getConfiguration();
        invocationId = invocationParameters.getAppId();

        //This happens under _jobLock, so we wont interfere with the running tasks

        runMapContext = new RunHadoopMapContext<OUTKEY, OUTVALUE>(invocationParameters.getHadoopPartitionToSossRegionMapping(),
                invocationParameters.getAppId(),
                HServerParameters.getSetting(MAP_OUTPUTCHUNKSIZE_KB, configuration),
                HServerParameters.getSetting(MAP_HASHTABLESIZE, configuration),
                HServerParameters.getSetting(MAP_MAXTEMPMEMORY_KB, configuration),
                mapperWrapper.getMapOutputKeyClass(),
                mapperWrapper.getMapOutputValueClass(),
                false,
                0,
                mapperWrapper,
                mapperWrapper.getPartitioner(),
                configuration);

        isSingleResultOptimisation = invocationParameters.isSingleResultOptimisation();

        inputSplitList = invocationParameters.getInputSplits();

        List<Integer> splitIndexList = null;
        for (InetAddress address : NetUtils.getLocalInterfaces()) {
            splitIndexList = ((InvocationParameters<?>) invocationParameters).getInputSplitAssignment().get(address);
            if (splitIndexList != null){

                //Handle the workload sharing between multiple JVMs
                //We assume that split list for the IP comes to each JVM in the same order
                if (InvocationWorker.getNumberOfWorkers() > 1) {
                    int listSize = splitIndexList.size();
                    int splitsPerHost = Math.max(1, listSize / InvocationWorker.getNumberOfWorkers()+1);
                    int startIndex = splitsPerHost * InvocationWorker.getIgWorkerIndex();

                    if (startIndex < listSize) {
                        int stopIndex = Math.min(listSize, startIndex + splitsPerHost);
                        splitIndexList = splitIndexList.subList(startIndex, stopIndex);
                    } else {
                        splitIndexList = Collections.EMPTY_LIST;
                    }
                    _logger.warn("Split list to process:"+splitIndexList+", ;"+listSize+","+splitsPerHost+","+startIndex);
                }
                break;
            }
        }


        if (splitIndexList != null && splitIndexList.size()>0) {  //We found our split list
            numberOfSplits = splitIndexList.size();
            splitIndexesForThisHost = new ArrayBlockingQueue<Integer>(numberOfSplits);
            splitIndexesForThisHost.addAll(splitIndexList);
            numberOfWorkers = Math.max(1, Math.min(invocationParameters.getNumberOfSlotsPerNode(), splitIndexesForThisHost.size()));

        } else { //Short circuit the mapper
            numberOfSplits = 0;
            splitIndexesForThisHost = null;
            numberOfWorkers = 0;
        }

        //If there is a cap on maximum number of slots, apply it
        int maxSlots = HServerParameters.getSetting(HServerParameters.MAX_SLOTS, configuration);
        if(maxSlots>0)
        {
            numberOfWorkers = Math.min(numberOfWorkers, maxSlots);
        }

    }

    /**
     * Runs all the mappers for the host.
     * In case the combiner class is specified, an instance of {@link WrappingMapOutputAccumulator} is created
     * for each slot(worker thread). This combiners are returned by {@link #runSlot()} when all
     * splits are done and merged together by a merge tree {@link com.scaleoutsoftware.soss.client.util.MergeTree}.
     *
     * @return result of the mapper execution
     */
    @SuppressWarnings("unchecked")
    private MapperResult<OUTVALUE> execute() throws InterruptedException, ExecutionException, IOException {
        long timeStarted = System.currentTimeMillis();
        if (numberOfSplits == 0) {
            return new MapperResult<OUTVALUE>(invocationId, null, null, 0, System.currentTimeMillis() - timeStarted); //No work on that host
        }

        OUTVALUE optimisationResult = null;  //this will use the result in case of single result optimisation


        if (mapperWrapper.hasCombiner()) //Do a merge tree
        {
            WrappingMapOutputAccumulator finalCombiner = (new MergeTree<WrappingMapOutputAccumulator<OUTKEY, OUTVALUE>>(numberOfWorkers) {

                @Override
                public WrappingMapOutputAccumulator<OUTKEY, OUTVALUE> runTask() throws Exception {
                    return (WrappingMapOutputAccumulator<OUTKEY, OUTVALUE>) runSlot();
                }

                @Override
                public void doMerge(WrappingMapOutputAccumulator<OUTKEY, OUTVALUE> to, WrappingMapOutputAccumulator<OUTKEY, OUTVALUE> from) throws Exception {
                    to.mergeInKeyValuesFromAnotherCombiner(from);
                }
            }).execute(taskExecutor);
            finalCombiner.close(); //Will induce write to the server
            if (isSingleResultOptimisation) {
                optimisationResult = ((SingleKeyMapOutputAccumulator<OUTKEY, OUTVALUE>) finalCombiner).getAggregateValue();
                //Cache combiner to be retrieved during the optimisation merge step.
                combinerCacheByInvocationId.put(invocationId, (SingleKeyMapOutputAccumulator<OUTKEY, OUTVALUE>) finalCombiner);
            }

        } else //No intraslot combining on the client
        {
            List<Future> futures = new LinkedList<Future>();
            for (int i = 0; i < numberOfWorkers; i++) {
                futures.add(taskExecutor.submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        MapOutputAccumulator combiner = runSlot();
                        combiner.close();
                        return null;
                    }
                }));
            }
            for (Future future : futures) {
                future.get();
            }

        }

        //There will be no reduce phase, cleanup now
        if(invocationParameters.getHadoopPartitionToSossRegionMapping().length == 0)
        {
            HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), configuration).onJobDone(invocationParameters);
        }

        return new MapperResult<OUTVALUE>(invocationId, optimisationResult, optimisationResult != null ? runMapContext.getValueClass() : null, numberOfSplits, System.currentTimeMillis() - timeStarted);
    }

    public static class MapperInvokable implements Invokable<Integer, InvocationParameters, MapperResult> {
        private final static Object _jobLock = new Object(); //Allow only one job to run at a time

        @Override
        public MapperResult eval(Integer integer, InvocationParameters invocationParameters, EvalArgs<Integer> integerEvalArgs) throws InvokeException, InterruptedException {
            try {
                synchronized (_jobLock) {
                    RunMapper runMapper = new RunMapper(invocationParameters);
                    return runMapper.execute();
                }
            } catch (Exception e) {
                throw new InvokeException("Exception occurred while running mapper task", e);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public MapperResult merge(MapperResult r1, MapperResult r2) throws InvokeException, InterruptedException {
            try {
                return r1.mergeWithOther(r2);
            } catch (IOException e) {
                throw new InvokeException("Cannot merge results.", e);
            }

        }
    }

    /**
     * @return the combiner for the slot
     */
    private MapOutputAccumulator<OUTKEY, OUTVALUE> runSlot() throws Exception {

        MapOutputAccumulator<OUTKEY, OUTVALUE> combiner;
        if (isSingleResultOptimisation) {
            if (!mapperWrapper.hasCombiner()) {
                throw new IOException("Single result optimisation requires specifying a combiner.");
            }
            combiner = new SingleKeyMapOutputAccumulator<OUTKEY, OUTVALUE>(runMapContext);
        } else if (mapperWrapper.hasCombiner()) {
            try {
                DataGridWriterParameters<OUTKEY, OUTVALUE> params = new DataGridWriterParameters<OUTKEY, OUTVALUE>(
                        -1, // unknown
                        runMapContext.getInvocationId(),
                        false,
                        new WritableSerializerDeserializer<OUTKEY>(runMapContext.getKeyClass()),
                        new WritableSerializerDeserializer<OUTVALUE>(runMapContext.getValueClass()),
                        runMapContext,
                        -1 // unknown
                );
                combiner = new IncrementalMapOutputAccumulator<OUTKEY, OUTVALUE>(params);
            } catch (NoSuchMethodException e) {
                throw new IOException("Cannot instantiate the combiner", e);
            }
        } else {
            DataGridWriterParameters<OUTKEY, OUTVALUE> params = new DataGridWriterParameters<OUTKEY, OUTVALUE>(
                    -1, // unknown
                    runMapContext.getInvocationId(),
                    false,
                    new WritableSerializerDeserializer<OUTKEY>(runMapContext.getKeyClass()),
                    new WritableSerializerDeserializer<OUTVALUE>(runMapContext.getValueClass()),
                    runMapContext,
                    -1 // unknown
                    );
            combiner = new PassthruMapOutputAccumulator<OUTKEY, OUTVALUE>(params);
        }
        Integer splitIndex;
        while ((splitIndex = splitIndexesForThisHost.poll()) != null) {
            mapperWrapper.runSplit(combiner, inputSplitList.get(splitIndex), splitIndex);
        }

        return combiner;
    }

}
