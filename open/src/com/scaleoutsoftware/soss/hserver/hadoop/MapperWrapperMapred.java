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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * This class contains the access layer for the mapper for the old (mapred) api
 */
public class MapperWrapperMapred<INKEY, INVALUE, OUTKEY, OUTVALUE> implements MapperWrapper<INKEY, INVALUE, OUTKEY, OUTVALUE> {
    public static final Log LOG =
            LogFactory.getLog(MapperWrapperMapred.class);

    private final JobConf jobConf;
    private final HServerInvocationParameters invocationParameters;
    private final JobID jobId;
    private final JobContext jobContext;
    private final Class<? extends org.apache.hadoop.mapred.Reducer> combinerClass;
    private final HadoopVersionSpecificCode hadoopVersionSpecificCode;
    private final Constructor mapperConstructor;
    private final Class<? extends org.apache.hadoop.mapred.Partitioner> partitionerClass;
    private final boolean mapOnlyJob;


    public MapperWrapperMapred(HServerInvocationParameters invocationParameters) throws IOException, ClassNotFoundException, NoSuchMethodException {

        this.invocationParameters = invocationParameters;
        jobConf = (JobConf) invocationParameters.getConfiguration();

        LOG.info("Starting mapper:"+HadoopInvocationParameters.dumpConfiguration(jobConf));

        hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), jobConf);
        hadoopVersionSpecificCode.onJobInitialize(invocationParameters);

        jobId = (JobID) invocationParameters.getJobId();

        jobContext = hadoopVersionSpecificCode.createJobContext(jobConf, jobId);

        combinerClass = jobConf.getCombinerClass();

        //Create constructor to save time on mapper instantiations
        mapperConstructor = jobConf.getMapperClass().getConstructor(new Class[]{});
        mapperConstructor.setAccessible(true);

        partitionerClass = jobConf.getPartitionerClass();

        mapOnlyJob = invocationParameters.getHadoopPartitionToSossRegionMapping().length == 0 && !invocationParameters.isSingleResultOptimisation(); //Mapper output goes straight to Output Format
    }

    /**
     * Runs mapper for the single split.
     *
     * @param mapOutputAccumulator mapOutputAccumulator to use
     * @param split                split ot run on
     */

    @Override
    @SuppressWarnings("unchecked")
    public void runSplit(final MapOutputAccumulator<OUTKEY, OUTVALUE> mapOutputAccumulator, Object split, int splitIndex)
            throws IOException, ClassNotFoundException, InterruptedException {
        JobConf jobConf = new JobConf(this.jobConf); //Clone JobConf to prevent unexpected task interaction

        TaskAttemptID taskAttemptID = TaskAttemptID.downgrade(hadoopVersionSpecificCode.createTaskAttemptId(jobId, true, splitIndex));

        ReducerWrapperMapred.updateJobConf(jobConf,taskAttemptID,splitIndex);
        updateJobWithSplit(jobConf, split);


        InputFormat inputFormat = jobConf.getInputFormat();

        Reporter reporter = Reporter.NULL;

        //Create RecordReader
        org.apache.hadoop.mapred.RecordReader<INKEY, INVALUE> recordReader = inputFormat.getRecordReader((InputSplit) split, jobConf, reporter);

        //Make a mapper
        org.apache.hadoop.mapred.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper;
        try {
            mapper = (org.apache.hadoop.mapred.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>) mapperConstructor.newInstance();
            mapper.configure(jobConf);
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate mapper " + mapperConstructor.getDeclaringClass(), e);
        }

        //These are to support map only jobs which write output directly to HDFS.
        final RecordWriter outputRecordWriter;
        OutputCommitter outputCommitter = null;
        TaskAttemptContext taskAttemptContext = null;

        if (mapOnlyJob) {

            taskAttemptContext = hadoopVersionSpecificCode.createTaskAttemptContextMapred(jobConf, taskAttemptID);
            OutputFormat outputFormat = jobConf.getOutputFormat();
            FileSystem fs = FileSystem.get(jobConf);
            outputRecordWriter = (org.apache.hadoop.mapred.RecordWriter<OUTKEY, OUTVALUE>) outputFormat.getRecordWriter(fs, jobConf, ReducerWrapperMapred.getOutputName(splitIndex), Reporter.NULL);
            outputCommitter = jobConf.getOutputCommitter();

            //Create task object so it can handle file format initialization
            //The MapTask is private in the Hadoop 1.x so we have to go through reflection.
            try {
                Class reduceTask = Class.forName("org.apache.hadoop.mapred.MapTask");
                Constructor reduceTaskConstructor = reduceTask.getDeclaredConstructor(String.class, TaskAttemptID.class, int.class, JobSplit.TaskSplitIndex.class, int.class);
                reduceTaskConstructor.setAccessible(true);
                Task task = (Task) reduceTaskConstructor.newInstance(null, taskAttemptID, splitIndex, new JobSplit.TaskSplitIndex(), 0);
                task.setConf(jobConf);
                task.initialize(jobConf, jobId, Reporter.NULL, false);
            } catch (Exception e) {
                throw new IOException("Cannot initialize MapTask", e);
            }
            outputCommitter.setupTask(taskAttemptContext);
        } else {
            outputRecordWriter = null;
        }

        OutputCollector<OUTKEY, OUTVALUE> outputCollector;

        if (!mapOnlyJob) {
            outputCollector = new OutputCollector<OUTKEY, OUTVALUE>() {
                @Override
                public void collect(OUTKEY outkey, OUTVALUE outvalue) throws IOException {
                    try {
                        mapOutputAccumulator.combine(outkey, outvalue);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
        } else {
            outputCollector = new OutputCollector<OUTKEY, OUTVALUE>() {
                @Override
                public void collect(OUTKEY outkey, OUTVALUE outvalue) throws IOException {
                    outputRecordWriter.write(outkey, outvalue);
                }
            };
        }

        INKEY key = recordReader.createKey();
        INVALUE value = recordReader.createValue();

        while (recordReader.next(key, value)) {
            mapper.map(key, value, outputCollector, reporter);
        }
        mapper.close();

        recordReader.close();

        if (mapOnlyJob)
        {
            outputRecordWriter.close(Reporter.NULL);
            outputCommitter.commitTask(taskAttemptContext);
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
            return new ReducerWrapperMapred<OUTKEY, OUTVALUE, OUTKEY, OUTVALUE>(invocationParameters, consumer, combinerClass);
        }
    }


    @Override
    public PartitionerWrapper<OUTKEY, OUTVALUE> getPartitioner() {
        return new PartitionerWrapper<OUTKEY, OUTVALUE>(jobContext.getNumReduceTasks()) {
            org.apache.hadoop.mapred.Partitioner<OUTKEY, OUTVALUE> partitioner = (org.apache.hadoop.mapred.Partitioner<OUTKEY, OUTVALUE>)
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

    //Based on updateJobWithSplit(...) from MapTask.java in Apache Hadoop 2.2.0
    /**
     * Update the job with details about the file split
     * @param job the job configuration to update
     * @param inputSplit the file split
     */
    private void updateJobWithSplit(final JobConf job, Object inputSplit) {
        if (inputSplit instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            try {
                if (fileSplit.getPath() != null) {
                    job.set("mapreduce.map.input.file", fileSplit.getPath().toString());
                }
            } catch (IllegalArgumentException e) {
                //Swallow this, it appears in Hive splits, which do not have the path encoded
                //(storage handler for NamedMap is an example).
            }
            job.setLong("mapreduce.map.input.start", fileSplit.getStart());
            job.setLong("mapreduce.map.input.length", fileSplit.getLength());
        }
        LOG.info("Processing split: " + inputSplit);
    }
}
