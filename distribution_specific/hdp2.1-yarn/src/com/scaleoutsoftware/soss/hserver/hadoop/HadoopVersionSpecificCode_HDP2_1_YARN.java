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
package com.scaleoutsoftware.soss.hserver.hadoop;

import com.scaleoutsoftware.soss.hserver.InvocationParameters;
import com.scaleoutsoftware.soss.hserver.MapOutputAccumulator;
import com.scaleoutsoftware.soss.hserver.interop.KeyValueProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;


public class HadoopVersionSpecificCode_HDP2_1_YARN extends HadoopVersionSpecificCode {

    @Override
    public String getHadoopLibraryString() {
        return "hdp2.1-yarn";
    }

	@Override
    public TaskAttemptID createTaskAttemptId(JobID jobID, boolean isMapper, int hadoopPartition) {
        return new TaskAttemptID(new TaskID(jobID, isMapper, hadoopPartition), 0);
    }

    @Override
    public TaskAttemptContext createTaskAttemptContext(Configuration configuration, TaskAttemptID id) {
        return new TaskAttemptContextImpl(configuration, id);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(JobConf configuration, JobID jobID) {
        //Initialize the distributed cache
        return new org.apache.hadoop.mapred.JobContextImpl(configuration, jobID);
    }

    @Override
    public <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getReducerContext(Configuration configuration, TaskAttemptID id, OutputCommitter outputCommitter, RecordWriter<KEYOUT, VALUEOUT> output, KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport, MapOutputAccumulator<KEYOUT, VALUEOUT> consumer) throws IOException, InterruptedException {
        return (new WrappingReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>()).getReducerContext(configuration, id, outputCommitter, output, transport, consumer);
    }


    static class WrappingReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        public WrappingReducer() {
        }

        @SuppressWarnings("unchecked")
        public org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getReducerContext(Configuration configuration,
                                                                                                               TaskAttemptID id,
                                                                                                               OutputCommitter outputCommitter,
                                                                                                               RecordWriter<KEYOUT, VALUEOUT> output,
                                                                                                               KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport,
                                                                                                               MapOutputAccumulator<KEYOUT, VALUEOUT> consumer

        ) throws IOException, InterruptedException {
            return new WrappingContext(configuration, id, outputCommitter, output, transport, consumer);
        }

        public class WrappingContext extends org.apache.hadoop.mapreduce.Reducer.Context {
            ReduceContextImpl impl;
            KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport;
            MapOutputAccumulator<KEYOUT, VALUEOUT> mapOutputAccumulatorCallback;

            @SuppressWarnings("unchecked")
            WrappingContext(Configuration configuration,
                            TaskAttemptID id,
                            OutputCommitter outputCommitter,
                            RecordWriter<KEYOUT, VALUEOUT> output,
                            KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport,
                            MapOutputAccumulator<KEYOUT, VALUEOUT> consumer
            ) throws IOException, InterruptedException {
                //Override the actual key and value class with Writables, to ensure that constructor
                //will not throw exception if SerializationFactory does not support that class.
                //Any actual serialization/deserialization is performed by DataTransport, so this
                //factory is never used.

                super();
                impl = new ReduceContextImpl(configuration, id, new DummyRawIterator()
                        , null, null, output, outputCommitter, new TaskAttemptContextImpl.DummyReporter(), null, Writable.class, Writable.class);
                this.transport = transport;
                this.mapOutputAccumulatorCallback = consumer;
            }

            @Override
            public boolean nextKey() throws IOException, InterruptedException {
                try {
                    if (transport != null) {
                        return transport.readNext();
                    } else {
                        return false;
                    }
                } catch (TimeoutException e) {
                    throw new IOException("Read operation timed out.", e);
                }
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return impl.nextKeyValue();
            }

            @Override
            public KEYIN getCurrentKey() {
                return transport != null ? transport.getKey() : null;
            }

            @Override
            public Object getCurrentValue() {
                return impl.getCurrentValue();
            }


            @Override
            public void write(Object key, Object value) throws IOException, InterruptedException {
                if (mapOutputAccumulatorCallback != null) {
                    mapOutputAccumulatorCallback.saveCombineResult((KEYOUT) key, (VALUEOUT) value);
                } else {
                    impl.write(key, value);
                }
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return impl.getOutputCommitter();
            }

            //@Override
            public boolean userClassesTakesPrecedence() {
                return true;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return impl.getTaskAttemptID();
            }

            @Override
            public String getStatus() {
                return impl.getStatus();
            }

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return impl.getCounter(counterName);
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return impl.getCounter(groupName, counterName);
            }

            @Override
            public void progress() {
                //impl.progress();
            }

            @Override
            public void setStatus(String status) {
                impl.setStatus(status);
            }

//            @Override
//            public float getProgress() {
//                return impl.getProgress();
//            }

            @Override
            public Configuration getConfiguration() {
                return impl.getConfiguration();
            }

            @Override
            public JobID getJobID() {
                return impl.getJobID();
            }

            @Override
            public int getNumReduceTasks() {
                return impl.getNumReduceTasks();
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                return impl.getWorkingDirectory();
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return impl.getOutputKeyClass();
            }

            @Override
            public Class<?> getOutputValueClass() {
                return impl.getOutputValueClass();
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return impl.getMapOutputKeyClass();
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return impl.getMapOutputValueClass();
            }

            @Override
            public String getJobName() {
                return impl.getJobName();
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                return impl.getInputFormatClass();
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                return impl.getMapperClass();
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                return impl.getCombinerClass();
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                return impl.getReducerClass();
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                return impl.getOutputFormatClass();
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                return impl.getPartitionerClass();
            }

            @Override
            public RawComparator<?> getSortComparator() {
                return impl.getSortComparator();
            }

            @Override
            public String getJar() {
                return impl.getJar();
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                return impl.getGroupingComparator();
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                return impl.getJobSetupCleanupNeeded();
            }

//            @Override
//            public boolean getTaskCleanupNeeded() {
//                return impl.getTaskCleanupNeeded();
//            }

            @Override
            public boolean getSymlink() {
                return impl.getSymlink();
            }

            @Override
            public Path[] getArchiveClassPaths() {
                return impl.getArchiveClassPaths();
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                return impl.getCacheArchives();
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                return impl.getCacheFiles();
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return impl.getLocalCacheArchives();
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return impl.getLocalCacheFiles();
            }

            @Override
            public Path[] getFileClassPaths() {
                return impl.getFileClassPaths();
            }

            @Override
            public String[] getArchiveTimestamps() {
                return impl.getArchiveTimestamps();
            }

            @Override
            public String[] getFileTimestamps() {
                return impl.getFileTimestamps();
            }

            @Override
            public int getMaxMapAttempts() {
                return impl.getMaxMapAttempts();
            }

            @Override
            public int getMaxReduceAttempts() {
                return impl.getMaxReduceAttempts();
            }

            @Override
            public boolean getProfileEnabled() {
                return impl.getProfileEnabled();
            }

            @Override
            public String getProfileParams() {
                return impl.getProfileParams();
            }

//            @Override
//            public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
//                return impl.getProfileTaskRange(isMap);
//            }

            @Override
            public String getUser() {
                return impl.getUser();
            }

            @Override
            public Credentials getCredentials() {
                return impl.getCredentials();
            }

            @Override
            @SuppressWarnings("unchecked")
            public Iterable getValues() throws IOException, InterruptedException {
                return transport != null ? transport.getValue() : null;
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return impl.getCombinerKeyGroupingComparator();
            }

            @Override
            public float getProgress() {
                return impl.getProgress();
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                return impl.getTaskCleanupNeeded();
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
                return impl.getProfileTaskRange(b);
            }

            public void setJobID(JobID jobId) {
                impl.setJobID(jobId);
            }
        }

    }


    @Override
    public <INKEY, INVALUE, OUTKEY, OUTVALUE> Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context getMapperContext(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException {
        return new MapperContextHolder(configuration, taskid, reader, writer).getContext();
    }

    /**
     * This class overrides mapper, to provide dummy context for user-defined mapper invocation.
     */
    static class MapperContextHolder extends org.apache.hadoop.mapreduce.Mapper {
        private HServerContext context;

        @SuppressWarnings("unchecked")
        public class HServerContext extends Context {
            MapContextImpl impl;

            public HServerContext(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException {
                impl = new MapContextImpl(configuration, taskid, reader, writer, null, new TaskAttemptContextImpl.DummyReporter(), null);
            }

            public void setJobID(JobID jobId) {
                impl.setJobID(jobId);
            }

            @Override
            public InputSplit getInputSplit() {
                return impl.getInputSplit();
            }

            @Override
            public Object getCurrentKey() throws IOException, InterruptedException {
                return impl.getCurrentKey();
            }

            //@Override
            public boolean userClassesTakesPrecedence() {
                return true;
            }

            @Override
            public Object getCurrentValue() throws IOException, InterruptedException {
                return impl.getCurrentValue();
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return impl.nextKeyValue();
            }

            @Override
            public void write(Object key, Object value) throws IOException, InterruptedException {
                impl.write(key, value);
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return impl.getOutputCommitter();
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return impl.getTaskAttemptID();
            }

            @Override
            public String getStatus() {
                return impl.getStatus();
            }

            @Override
            public Counter getCounter(Enum<?> counterName) {
                return impl.getCounter(counterName);
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                return impl.getCounter(groupName, counterName);
            }

            @Override
            public void progress() {
                impl.progress();
            }

            @Override
            public void setStatus(String status) {
                impl.setStatus(status);
            }

//            @Override
//            public float getProgress() {
//                return impl.getProgress();
//            }

            @Override
            public Configuration getConfiguration() {
                return impl.getConfiguration();
            }

            @Override
            public JobID getJobID() {
                return impl.getJobID();
            }

            @Override
            public int getNumReduceTasks() {
                return impl.getNumReduceTasks();
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                return impl.getWorkingDirectory();
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return impl.getOutputKeyClass();
            }

            @Override
            public Class<?> getOutputValueClass() {
                return impl.getOutputValueClass();
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return impl.getMapOutputKeyClass();
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return impl.getMapOutputValueClass();
            }

            @Override
            public String getJobName() {
                return impl.getJobName();
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                return impl.getInputFormatClass();
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                return impl.getMapperClass();
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                return impl.getCombinerClass();
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                return impl.getReducerClass();
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                return impl.getOutputFormatClass();
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                return impl.getPartitionerClass();
            }

            @Override
            public RawComparator<?> getSortComparator() {
                return impl.getSortComparator();
            }

            @Override
            public String getJar() {
                return impl.getJar();
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                return impl.getGroupingComparator();
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                return impl.getJobSetupCleanupNeeded();
            }

//            @Override
//            public boolean getTaskCleanupNeeded() {
//                return impl.getTaskCleanupNeeded();
//            }

            @Override
            public boolean getSymlink() {
                return impl.getSymlink();
            }

            @Override
            public Path[] getArchiveClassPaths() {
                return impl.getArchiveClassPaths();
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                return impl.getCacheArchives();
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                return impl.getCacheFiles();
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return impl.getLocalCacheArchives();
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return impl.getLocalCacheFiles();
            }

            @Override
            public Path[] getFileClassPaths() {
                return impl.getFileClassPaths();
            }

            @Override
            public String[] getArchiveTimestamps() {
                return impl.getArchiveTimestamps();
            }

            @Override
            public String[] getFileTimestamps() {
                return impl.getFileTimestamps();
            }

            @Override
            public int getMaxMapAttempts() {
                return impl.getMaxMapAttempts();
            }

            @Override
            public int getMaxReduceAttempts() {
                return impl.getMaxReduceAttempts();
            }

            @Override
            public boolean getProfileEnabled() {
                return impl.getProfileEnabled();
            }

            @Override
            public String getProfileParams() {
                return impl.getProfileParams();
            }

//            @Override
//            public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
//                return impl.getProfileTaskRange(isMap);
//            }

            //@Override
            public String getUser() {
                return impl.getUser();
            }

            @Override
            public Credentials getCredentials() {
                return impl.getCredentials();
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return impl.getCombinerKeyGroupingComparator();
            }

            @Override
            public float getProgress() {
                return impl.getProgress();
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                return impl.getTaskCleanupNeeded();
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
                return impl.getProfileTaskRange(b);
            }
        }

        public MapperContextHolder(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException {
            context = new HServerContext(configuration, taskid, reader, writer);
        }

        public HServerContext getContext() {
            return context;
        }
    }
    //----------------------------MAPRED------------------


    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContextMapred(JobConf jobConf, org.apache.hadoop.mapred.TaskAttemptID taskId) {
        try {

            Constructor<org.apache.hadoop.mapred.TaskAttemptContextImpl> contextConstructor = org.apache.hadoop.mapred.TaskAttemptContextImpl.class.getDeclaredConstructor(JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class);
            contextConstructor.setAccessible(true);
            return contextConstructor.newInstance(jobConf, taskId);
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate TaskAttemptContext.", e);
        }
    }

    //------------------INITILIZE AND TEARDOWN


    private static ConcurrentHashMap<JobID, DistributedCacheManager> distributedCaches = new ConcurrentHashMap<JobID, DistributedCacheManager>();
    @Override
    public void onJobInitialize(InvocationParameters parameters) throws IOException {
        //Take this chance to stub out ResourceCalculatorProcessTree
        parameters.getConfiguration().setClass(MRConfig.RESOURCE_CALCULATOR_PROCESS_TREE, DummyResourceCalculatorProcessTree.class, ResourceCalculatorProcessTree.class);

        //Initialize the distributed cache
        DistributedCacheManager cacheManager = new DistributedCacheManager();
        cacheManager.setup(parameters.getConfiguration());
        distributedCaches.put(parameters.getJobId(), cacheManager);

        super.onJobInitialize(parameters);
    }

    @Override
    public void onJobDone(InvocationParameters parameters) throws IOException {
        DistributedCacheManager cacheManager = distributedCaches.get(parameters.getJobId());
        if(cacheManager != null)
        {
            cacheManager.close();
            distributedCaches.remove(parameters.getJobId());
        }
        super.onJobDone(parameters);
    }
}
