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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;
import java.util.concurrent.TimeoutException;

import static com.scaleoutsoftware.soss.hserver.HServerParameters.*;

/**
 * This class contains the access layer for the {@link Reducer}.
 */
public class ReducerWrapperMapred<INKEY, INVALUE, OUTKEY, OUTVALUE> implements ReducerWrapper<INKEY, INVALUE, OUTKEY, OUTVALUE> {
    public static final Log LOG =
            LogFactory.getLog(MapperWrapperMapred.class);


    //---------------------------------------------------------------------------------
    //Based on the property names from MRJobConfig.java, part of Apache Hadoop 2.2.0,
    //licensed under Apache License, Version 2.0
    //----------------------------------------------------------------------------------
    public static final String TASK_ID = "mapreduce.task.id";
    public static final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";
    public static final String TASK_ISMAP = "mapreduce.task.ismap";
    public static final String TASK_PARTITION = "mapreduce.task.partition";
    public static final String ID = "mapreduce.job.id";
    //-------------------------------------------------------------------

    private  TaskAttemptContext context;
    private org.apache.hadoop.mapred.RecordWriter<OUTKEY, OUTVALUE> recordWriter;
    private OutputCommitter committer;
    private final org.apache.hadoop.mapred.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
    private final OutputCollector<OUTKEY, OUTVALUE> outputCollector;
    private DataGridChunkedCollectionReader<INKEY, INVALUE> transport;
    private int hadoopPartition;
    private HadoopVersionSpecificCode hadoopVersionSpecificCode;
    private InvocationParameters invocationParameters;


    public ReducerWrapperMapred(InvocationParameters invocationParameters, int hadoopPartition, int appId, int region, boolean sort) throws IOException, ClassNotFoundException, InterruptedException {
        this.invocationParameters = invocationParameters;
        JobConf jobConf = new JobConf(invocationParameters.getConfiguration());  //Clone JobConf, so the temporary settings do not pollute other tasks

        LOG.info("Starting reducer:"+InvocationParameters.dumpConfiguration(jobConf));

        JobID jobID = (JobID) invocationParameters.getJobId();
        this.hadoopPartition = hadoopPartition;
        hadoopVersionSpecificCode = HadoopVersionSpecificCode.getInstance(invocationParameters.getHadoopVersion(), jobConf);

        TaskAttemptID taskAttemptID = TaskAttemptID.downgrade(hadoopVersionSpecificCode.createTaskAttemptId(jobID, false, hadoopPartition));

        updateJobConf(jobConf, taskAttemptID, region);

        context = hadoopVersionSpecificCode.createTaskAttemptContextMapred(jobConf, taskAttemptID);

        reducer = (org.apache.hadoop.mapred.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>)
                ReflectionUtils.newInstance(jobConf.getReducerClass(), jobConf);

        reducer.configure(jobConf);

        OutputFormat outputFormat = jobConf.getOutputFormat();

        FileSystem fs = FileSystem.get(jobConf);
        recordWriter = (org.apache.hadoop.mapred.RecordWriter<OUTKEY, OUTVALUE>) outputFormat.getRecordWriter(fs, jobConf, getOutputName(hadoopPartition), Reporter.NULL);

        committer = jobConf.getOutputCommitter();

        //Create task object so it can handle file format initialization
        //The ReduceTask is private in the Hadoop 1.x so we have to go through reflection.
        try {
            Class reduceTask = Class.forName("org.apache.hadoop.mapred.ReduceTask");
            Constructor reduceTaskConstructor = reduceTask.getDeclaredConstructor(String.class, TaskAttemptID.class, int.class, int.class, int.class);
            reduceTaskConstructor.setAccessible(true);
            Task task = (Task) reduceTaskConstructor.newInstance(null, taskAttemptID, hadoopPartition, 0, 0);
            task.setConf(jobConf);
            task.initialize(jobConf, jobID, Reporter.NULL, false);
        } catch (Exception e) {
            throw new IOException("Cannot initialize ReduceTask", e);
        }

        committer.setupTask(context);

        Class<INKEY> keyClass = (Class<INKEY>) jobConf.getMapOutputKeyClass();
        WritableSerializerDeserializer<INKEY> firstKeySerializer = new WritableSerializerDeserializer<INKEY>(keyClass);
        WritableSerializerDeserializer<INKEY> secondKeySerializer = new WritableSerializerDeserializer<INKEY>(keyClass);
        Class<INVALUE> valueClass = (Class<INVALUE>) jobConf.getMapOutputValueClass();
        WritableSerializerDeserializer<INVALUE> valueSerializer = new WritableSerializerDeserializer<INVALUE>(valueClass);

        DataGridReaderParameters<INKEY,INVALUE> params = new DataGridReaderParameters<INKEY, INVALUE>(
                region,
                appId,
                HServerParameters.getSetting(REDUCE_USEMEMORYMAPPEDFILES, jobConf) > 0,
                firstKeySerializer,
                valueSerializer,
                secondKeySerializer,
                keyClass,
                valueClass,
                sort,
                HServerParameters.getSetting(REDUCE_CHUNKSTOREADAHEAD, jobConf),
                1024 * HServerParameters.getSetting(REDUCE_INPUTCHUNKSIZE_KB, jobConf),
                HServerParameters.getSetting(REDUCE_CHUNKREADTIMEOUT, jobConf));
        transport = DataGridChunkedCollectionReader.getGridReader(params);
        outputCollector = new OutputCollector<OUTKEY, OUTVALUE>() {
            @Override
            public void collect(OUTKEY outkey, OUTVALUE outvalue) throws IOException {
                     recordWriter.write(outkey, outvalue);
            }
        };
    }


    @Override
    public void runReducer() throws IOException, InterruptedException {
        LOG.info("Starting reduce:"+hadoopPartition+","+getOutputName(hadoopPartition)+","+recordWriter);
        //Run the reducer
        try {
            while (transport.readNext()) {
                reducer.reduce(transport.getKey(), transport.getValue().iterator(), outputCollector, Reporter.NULL);
            }
            reducer.close();
            recordWriter.close(Reporter.NULL);
            committer.commitTask(context);
            LOG.info("Reduce done:"+hadoopPartition+","+getOutputName(hadoopPartition)+","+recordWriter);
        } catch (IOException e) {
            committer.abortTask(context);
            throw new IOException("Exception occurred during reduce: "+hadoopPartition+","+getOutputName(hadoopPartition)+","+recordWriter,e);
        } catch (TimeoutException e) {
            committer.abortTask(context);
            throw new IOException(e);
        } 
    }



    public ReducerWrapperMapred(InvocationParameters invocationParameters, final MapOutputAccumulator<OUTKEY, OUTVALUE> consumer, Class<? extends org.apache.hadoop.mapred.Reducer> combinerClass) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf jobConf = (JobConf) invocationParameters.getConfiguration();

        reducer = (org.apache.hadoop.mapred.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>)
                ReflectionUtils.newInstance(combinerClass, jobConf);

        outputCollector = new OutputCollector<OUTKEY, OUTVALUE>() {
            @Override
            public void collect(OUTKEY outkey, OUTVALUE outvalue) throws IOException {
                try {
                    consumer.saveCombineResult(outkey, outvalue);
                } catch (Exception e) {
                    throw new IOException("Error while saving combined result.", e);
                }
            }
        };


    }


    @Override
    public void reduce(INKEY key, Iterable<INVALUE> values) throws IOException {
        reducer.reduce(key, values.iterator(), outputCollector, Reporter.NULL);
    }

    //---------------------------------------------------------------------------------
    //Based on the partition name generation from Task.java, part of Apache Hadoop 1.2.0,
    //licensed under Apache License, Version 2.0
    //----------------------------------------------------------------------------------
    /**
     * Construct output file names so that, when an output directory listing is
     * sorted lexicographically, positions correspond to output partitions.
     */
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    static synchronized String getOutputName(int partition) {
        return "part-" + NUMBER_FORMAT.format(partition);
    }
    //---------------------------------------------------------------------------------------


    //Update JobConf with current task info, so IF/OF can use it.
    static void updateJobConf(JobConf jobConf, TaskAttemptID taskAttemptID, int partition)
    {

        //---------------------------------------------------------------------------------
        //Based on the localizeConfiguration(...) method from Task.java, part of Apache Hadoop 1.2.0,
        //licensed under Apache License, Version 2.0
        //----------------------------------------------------------------------------------

        jobConf.set("mapred.tip.id", taskAttemptID.getTaskID().toString());
        jobConf.set("mapred.task.id", taskAttemptID.toString());
        jobConf.setBoolean("mapred.task.is.map", false);
        jobConf.setInt("mapred.task.partition", partition);
        jobConf.set("mapred.job.id", taskAttemptID.getJobID().toString());

        //---------------------------------------------------------------------------------
        //Based on the localizeConfiguration(...) method from Task.java, part of Apache Hadoop 2.2.0,
        //licensed under Apache License, Version 2.0
        //----------------------------------------------------------------------------------
        jobConf.set(TASK_ID, taskAttemptID.getTaskID().toString());
        jobConf.set(TASK_ATTEMPT_ID, taskAttemptID.toString());
        jobConf.setBoolean(TASK_ISMAP, false);
        jobConf.setInt(TASK_PARTITION, partition);
        jobConf.set(ID, taskAttemptID.getJobID().toString());
        //----------------------------------------------------------------------------------
    }

}
