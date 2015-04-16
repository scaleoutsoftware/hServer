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

import com.scaleoutsoftware.soss.hserver.MapOutputAccumulator;
import com.scaleoutsoftware.soss.hserver.interop.KeyValueProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeoutException;


public class HadoopVersionSpecificCode_1x extends HadoopVersionSpecificCode {

    @Override
    public String getHadoopLibraryString() {
        return "hadoop-1.2.1";
    }

    @Override
    public TaskAttemptID createTaskAttemptId(JobID jobID, boolean isMapper, int hadoopPartition) {
        return new TaskAttemptID(new TaskID(jobID, isMapper, hadoopPartition), 0);
    }

    @Override
    public TaskAttemptContext createTaskAttemptContext(Configuration configuration, TaskAttemptID id) {
        return new TaskAttemptContext(configuration, id);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(JobConf jobConf, JobID jobId) {
        try {
            Constructor<org.apache.hadoop.mapred.JobContext> contextConstructor = org.apache.hadoop.mapred.JobContext.class.getDeclaredConstructor(JobConf.class, org.apache.hadoop.mapreduce.JobID.class);
            contextConstructor.setAccessible(true);
            return contextConstructor.newInstance(jobConf, jobId);
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate JobContextImpl.", e);
        }
    }

    @Override
    public <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Reducer.Context getReducerContext(Configuration configuration,
                                                                                TaskAttemptID id,
                                                                                OutputCommitter outputCommitter,
                                                                                RecordWriter<KEYOUT, VALUEOUT> output,
                                                                                KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport,
                                                                                MapOutputAccumulator<KEYOUT, VALUEOUT> combinerCallback

    ) throws IOException, InterruptedException {
        return (new WrappingReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>()).getReducerContext(configuration, id, outputCommitter, output, transport, combinerCallback);
    }

    /**
     * Subclass the reducer context to feed the reducer from the {@link com.scaleoutsoftware.soss.mapreduce.DataGridChunkedCollectionReader}.
     */
    static class WrappingReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        public WrappingReducer() {
        }

        @SuppressWarnings("unchecked")
        public org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getReducerContext(Configuration configuration,
                                                                                                               TaskAttemptID id,
                                                                                                               OutputCommitter outputCommitter,
                                                                                                               RecordWriter<KEYOUT, VALUEOUT> output,
                                                                                                               KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport,
                                                                                                               MapOutputAccumulator<KEYOUT, VALUEOUT> combinerCallback

        ) throws IOException, InterruptedException {
            return new WrappingContext(configuration, id, outputCommitter, output, transport, combinerCallback);
        }

        public class WrappingContext extends org.apache.hadoop.mapreduce.Reducer.Context {
            KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport;
            MapOutputAccumulator<KEYOUT, VALUEOUT> combinerCallback;

            @SuppressWarnings("unchecked")
            WrappingContext(Configuration configuration,
                            TaskAttemptID id,
                            OutputCommitter outputCommitter,
                            RecordWriter<KEYOUT, VALUEOUT> output,
                            KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport,
                            MapOutputAccumulator<KEYOUT, VALUEOUT> combinerCallback
            ) throws IOException, InterruptedException {
                //Override the actual key and value class with Writables, to ensure that constructor
                //will not throw exception if SerializationFactory does not support that class.
                //Any actual serialization/deserialization is performed by DataTransport, so this
                //factory is never used.

                super(configuration, id, new DummyRawIterator()
                        , null, null, output, outputCommitter, null, null, Writable.class, Writable.class);
                this.transport = transport;
                this.combinerCallback = combinerCallback;
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
                throw new RuntimeException("Operation not supported");
            }

            @Override
            public KEYIN getCurrentKey() {
                return transport != null ? transport.getKey() : null;
            }

            @Override
            public VALUEIN getCurrentValue() {
                throw new RuntimeException("Operation not supported");
            }


            @Override
            public void write(Object key, Object value) throws IOException, InterruptedException {
                if (combinerCallback != null) {
                    combinerCallback.saveCombineResult((KEYOUT) key, (VALUEOUT) value);
                } else {
                    super.write(key, value);
                }
            }

            @Override
            public Counter getCounter(String groupName, String counterName) {
                throw new RuntimeException("Operation not supported");
            }

            @Override
            public void progress() {
                //Do nothing
            }

            @Override
            public void setStatus(String status) {
                //Do nothing
            }

            @Override
            @SuppressWarnings("unchecked")
            public Iterable getValues() throws IOException, InterruptedException {
                return transport != null ? transport.getValue() : null;
            }
        }

    }


    @Override
    public <INKEY, INVALUE, OUTKEY, OUTVALUE> Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context getMapperContext(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException {
        return new MapperContextHolder(configuration, taskid, reader, writer).getContext();
    }


    /**
     * This class overrides mapper, to provide dummy context for user-defined mapper invocati//.
     */
    static class MapperContextHolder extends org.apache.hadoop.mapreduce.Mapper {
        private HServerContext context;

        @SuppressWarnings("unchecked")
        public class HServerContext extends Context {
            public HServerContext(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException {
                super(configuration, taskid, reader, writer, null, null, null);
            }
        }

        public MapperContextHolder(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException {
            context = new HServerContext(configuration, taskid, reader, writer);
        }

        public MapperContextHolder.HServerContext getContext() {
            return context;
        }
    }

    //-----------------------------------MAPRED----------------------------------------------------


    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContextMapred(JobConf jobConf, org.apache.hadoop.mapred.TaskAttemptID taskId) {
        try {
            Constructor<org.apache.hadoop.mapred.TaskAttemptContext> contextConstructor = org.apache.hadoop.mapred.TaskAttemptContext.class.getDeclaredConstructor(JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class);
            contextConstructor.setAccessible(true);
            return contextConstructor.newInstance(jobConf, taskId);
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate TaskAttemptContext.", e);
        }
    }

}
