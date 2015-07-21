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


import com.scaleoutsoftware.soss.hserver.HServerInvocationParameters;
import com.scaleoutsoftware.soss.hserver.InvocationParameters;
import com.scaleoutsoftware.soss.hserver.MapOutputAccumulator;
import com.scaleoutsoftware.soss.hserver.RunHadoopMapContext;
import com.scaleoutsoftware.soss.hserver.interop.RunMapContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * This class contains the access layer for the mapper. It subclasses the record writer to be able to forward
 * key-values and defines several dummy classes to be used as a stubs for the Hadoop infrastructure ones.
 */
public class MapperWrapperMapreduce<INKEY, INVALUE, OUTKEY, OUTVALUE> implements MapperWrapper<INKEY,INVALUE,OUTKEY,OUTVALUE> {
    private final Configuration configuration;
    private final HServerInvocationParameters invocationParameters;
    private final JobID jobId;
    private final JobContext jobContext;
    private final Class<? extends org.apache.hadoop.mapreduce.Reducer> combinerClass;
    private final HadoopVersionSpecificCode hadoopVersionSpecificCode;
    private final Constructor mapperConstructor;
    private Class<? extends org.apache.hadoop.mapreduce.Partitioner> partitionerClass;
    private final boolean mapOnlyJob;

    public MapperWrapperMapreduce(HServerInvocationParameters invocationParameters) throws IOException, ClassNotFoundException, NoSuchMethodException {
        this.invocationParameters = invocationParameters;
        configuration = (Configuration)invocationParameters.getConfiguration();
        hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), configuration);
        hadoopVersionSpecificCode.onJobInitialize(invocationParameters);

        jobId = (JobID)invocationParameters.getJobId();

        jobContext = hadoopVersionSpecificCode.createJobContext(new JobConf(configuration), jobId);
        combinerClass = jobContext.getCombinerClass();

        //Create constructor to save time on mapper instantiations
        mapperConstructor = jobContext.getMapperClass().getConstructor(new Class[]{});
        mapperConstructor.setAccessible(true);

        partitionerClass = jobContext.getPartitionerClass();

        mapOnlyJob = invocationParameters.getHadoopPartitionToSossRegionMapping().length == 0 && !invocationParameters.isSingleResultOptimisation(); //Mapper output goes straight to Output Format
    }

    /**
     * Runs mapper for the single split.
     *
     * @param mapOutputAccumulator mapOutputAccumulator to use
     * @param split    split ot run on
     */

    @Override
    @SuppressWarnings("unchecked")
    public void runSplit(MapOutputAccumulator<OUTKEY, OUTVALUE> mapOutputAccumulator, Object split, int splitIndex)
            throws IOException, ClassNotFoundException, InterruptedException {

        TaskAttemptID taskAttemptId = hadoopVersionSpecificCode.createTaskAttemptId(jobId, true, splitIndex);
        //Setup task ID info
        TaskAttemptContext taskContext = hadoopVersionSpecificCode.createTaskAttemptContext(configuration, taskAttemptId);

        InputFormat inputFormat = ReflectionUtils.newInstance(jobContext.getInputFormatClass(), configuration);

        //Create RecordReader
        org.apache.hadoop.mapreduce.RecordReader<INKEY, INVALUE> input = inputFormat.createRecordReader((InputSplit) split, taskContext);

        //Make a mapper
        org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper;
        try {
            mapper = (org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>) mapperConstructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        org.apache.hadoop.mapreduce.RecordWriter output;
        OutputCommitter committer = null;
        if (mapOnlyJob) {
            OutputFormat outputFormat =
                    ReflectionUtils.newInstance(jobContext.getOutputFormatClass(), configuration);
            output = (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE>) outputFormat
                    .getRecordWriter(taskContext);
            committer = outputFormat.getOutputCommitter(taskContext);
            committer.setupTask(taskContext);
        } else {
            output = new MapOutputCollector<OUTKEY, OUTVALUE>(mapOutputAccumulator);
        }

        input.initialize((InputSplit) split, taskContext);

        org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mapperContext = hadoopVersionSpecificCode.getMapperContext(configuration, taskAttemptId, input, output);
        mapper.run(mapperContext);

        input.close();

        output.close(mapperContext);

        if (mapOnlyJob && committer != null) {
            committer.commitTask(taskContext);
        }
    }


    @Override
    public Class<OUTKEY> getMapOutputKeyClass() {
        return (Class<OUTKEY>) jobContext.getMapOutputKeyClass();
    }


    @Override
    public Class<OUTVALUE> getMapOutputValueClass() {
        return (Class<OUTVALUE>) jobContext.getMapOutputValueClass();
    }


    @Override
    public ReducerWrapper<OUTKEY, OUTVALUE, OUTKEY, OUTVALUE> getCombiner(MapOutputAccumulator<OUTKEY, OUTVALUE> consumer, RunMapContext<OUTKEY, OUTVALUE> mapContext) throws IOException, ClassNotFoundException, InterruptedException {
        if (combinerClass == null) {
            return null;
        } else {
            return new ReducerWrapperMapreduce<OUTKEY, OUTVALUE, OUTKEY, OUTVALUE>(invocationParameters, consumer, combinerClass);
        }
    }


    @Override
    public PartitionerWrapper<OUTKEY, OUTVALUE> getPartitioner() {
        return new PartitionerWrapper<OUTKEY, OUTVALUE>(jobContext.getNumReduceTasks()) {
            org.apache.hadoop.mapreduce.Partitioner<OUTKEY, OUTVALUE> partitioner = (org.apache.hadoop.mapreduce.Partitioner<OUTKEY, OUTVALUE>)
                    ReflectionUtils.newInstance(partitionerClass, jobContext.getConfiguration());

            @Override
            public int getPartition(OUTKEY key, OUTVALUE value) {
                return partitioner.getPartition(key, value, numberOfPartitions);
            }
        };
    }


    @Override
    public boolean hasCombiner() {
        return combinerClass != null;
    }

    /**
     * This output collector forwards map output to the appropriate mapOutputAccumulator.
     */
    private class MapOutputCollector<K, V> extends RecordWriter<K, V> {
        MapOutputAccumulator<K, V> mapOutputAccumulator;

        MapOutputCollector(MapOutputAccumulator<K, V> mapOutputAccumulator) {
            this.mapOutputAccumulator = mapOutputAccumulator;
        }

        @Override
        public void write(K k, V v) throws IOException, InterruptedException {
            mapOutputAccumulator.combine(k, v);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        }
    }


}
