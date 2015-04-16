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


import com.scaleoutsoftware.soss.hserver.*;
import com.scaleoutsoftware.soss.hserver.interop.DataGridChunkedCollectionReader;
import com.scaleoutsoftware.soss.hserver.interop.DataGridReaderParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.*;

/**
 * This class contains the access layer for the {@link Reducer}. It subclasses the reducer context
 * so the reducer gets the key-values from the appropriate {@link DataGridChunkedCollectionReader}.
 */
public class ReducerWrapperMapreduce<INKEY, INVALUE, OUTKEY, OUTVALUE> implements ReducerWrapper<INKEY,INVALUE,OUTKEY,OUTVALUE> {
    private final org.apache.hadoop.mapreduce.Reducer.Context context;
    private org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> recordWriter;
    private OutputCommitter committer;
    private final org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
    private final org.apache.hadoop.mapreduce.TaskAttemptContext taskContext;
    private final HadoopVersionSpecificCode hadoopVersionSpecificCode;
    protected Method reduceMethod;
    private InvocationParameters invocationParameters;


    public ReducerWrapperMapreduce(InvocationParameters invocationParameters, int hadoopPartition, int appId, int region, boolean sort) throws IOException, ClassNotFoundException, InterruptedException {
        this.invocationParameters = invocationParameters;
        Configuration configuration = invocationParameters.getConfiguration();
        hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), configuration);
        JobID jobID = invocationParameters.getJobId();

        //Setup task ID info
        TaskAttemptID id = hadoopVersionSpecificCode.createTaskAttemptId(jobID, false, hadoopPartition);
        JobContext jobContext = hadoopVersionSpecificCode.createJobContext(new JobConf(configuration), jobID);
        taskContext = hadoopVersionSpecificCode.createTaskAttemptContext(configuration, id);

        reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>)
                ReflectionUtils.newInstance(jobContext.getReducerClass(), configuration);

        OutputFormat outputFormat =
                ReflectionUtils.newInstance(jobContext.getOutputFormatClass(), configuration);

        recordWriter = (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE>) outputFormat
                .getRecordWriter(taskContext);


        committer = outputFormat.getOutputCommitter(taskContext);
        committer.setupTask(taskContext);

        Class<INKEY> keyClass = (Class<INKEY>) jobContext.getMapOutputKeyClass();
        WritableSerializerDeserializer<INKEY> firstKeySerializer = new WritableSerializerDeserializer<INKEY>(keyClass);
        WritableSerializerDeserializer<INKEY> secondKeySerializer = new WritableSerializerDeserializer<INKEY>(keyClass);
        Class<INVALUE> valueClass = (Class<INVALUE>) jobContext.getMapOutputValueClass();
        WritableSerializerDeserializer<INVALUE> valueSerializer = new WritableSerializerDeserializer<INVALUE>(valueClass);

        DataGridReaderParameters<INKEY,INVALUE> params = new DataGridReaderParameters<INKEY, INVALUE>(
                region,
                appId,
                HServerParameters.getSetting(REDUCE_USEMEMORYMAPPEDFILES, configuration) > 0,
                firstKeySerializer,
                valueSerializer,
                secondKeySerializer,
                keyClass,
                valueClass,
                sort,
                HServerParameters.getSetting(REDUCE_CHUNKSTOREADAHEAD, configuration),
                1024 * HServerParameters.getSetting(REDUCE_INPUTCHUNKSIZE_KB, configuration),
                HServerParameters.getSetting(REDUCE_CHUNKREADTIMEOUT, configuration));
        DataGridChunkedCollectionReader<INKEY, INVALUE> transport = DataGridChunkedCollectionReader.getGridReader(params);

        context = hadoopVersionSpecificCode.getReducerContext
                (configuration, id, committer, recordWriter, transport, null);

    }


    @Override
    public void runReducer() throws IOException, InterruptedException {
        //Run the reducer
        try {
            reducer.run(context);
            recordWriter.close(context);
            committer.commitTask(context);
        } catch (IOException e) {
            committer.abortTask(taskContext);
            throw e;
        }
    }


    public ReducerWrapperMapreduce(InvocationParameters invocationParameters, MapOutputAccumulator<OUTKEY, OUTVALUE> consumer, Class<? extends org.apache.hadoop.mapreduce.Reducer> combinerClass) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = invocationParameters.getConfiguration();
        hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), configuration);
        JobID jobID = invocationParameters.getJobId();

        //Setup task ID info
        TaskAttemptID id = hadoopVersionSpecificCode.createTaskAttemptId(jobID, false, 0);
        taskContext = hadoopVersionSpecificCode.createTaskAttemptContext(configuration, id);

        reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>)
                ReflectionUtils.newInstance(combinerClass, configuration);

        context = hadoopVersionSpecificCode.getReducerContext
                (configuration, id, null, null, null, consumer);

        try {
            reduceMethod = reducer.getClass().getDeclaredMethod("reduce", Object.class, Iterable.class, org.apache.hadoop.mapreduce.Reducer.Context.class);
            reduceMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IOException("Cannot find reduce method in the combiner.", e);
        }

    }


    @Override
    public void reduce(INKEY key, Iterable<INVALUE> values) throws IOException {
        try {
            reduceMethod.invoke(reducer, key, values, context);
        } catch (Exception e) {
            throw new IOException("Error occurred while calling the reducer.", e);
        }
    }




}
