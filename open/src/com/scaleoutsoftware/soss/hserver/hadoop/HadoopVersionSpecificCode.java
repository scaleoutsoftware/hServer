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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract base class for classes encapsulating version specific code. Each implementation corresponds
 * to particular supported Hadoop version.
 */
public abstract class HadoopVersionSpecificCode {
    private final static String HADOOP_VERSION_SPECIFIC_CODE = "com.scaleoutsoftware.soss.hserver.hadoop.HadoopVersionSpecificCode";

    /**
     * Abstract class for identifying Hadoop distribution by version string,
     * and matching it with a class name.
     */
    private static abstract class HadoopDistribution {
        private String className;

        protected HadoopDistribution(String suffix) {
            this.className = HADOOP_VERSION_SPECIFIC_CODE + "_" + suffix;
        }

        private String getClassName() {
            return className;
        }

        /**
         * Checks if version string points to that distribution, returns <code>true</code>
         * if this is the case.
         *
         * @param hadoopVersion Hadoop version string
         * @return true if distribution matches
         */
        abstract boolean check(String hadoopVersion, Configuration configuration);

        boolean isYARN(Configuration configuration)
        {
            return configuration.get("mapreduce.framework.name","").contains("yarn");
        }
    }

    private final static List<HadoopDistribution> _hadoopDistributions;

    static {
        _hadoopDistributions = new LinkedList<HadoopDistribution>();
		
		_hadoopDistributions.add(new HadoopDistribution("CDH4") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return !isYARN(configuration)&&hadoopVersion.toLowerCase().contains("cdh4");
            }
        });
		
		_hadoopDistributions.add(new HadoopDistribution("CDH5") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return !isYARN(configuration)&&hadoopVersion.toLowerCase().contains("2.3.0-cdh5.0.2");
            }
        });
		
		_hadoopDistributions.add(new HadoopDistribution("CDH5_YARN") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return isYARN(configuration)&&hadoopVersion.toLowerCase().contains("2.3.0-cdh5.0.2");
            }
        });

        _hadoopDistributions.add(new HadoopDistribution("CDH5_2") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return !isYARN(configuration)&&hadoopVersion.toLowerCase().contains("2.5.0-cdh5.2.1");
            }
        });

        _hadoopDistributions.add(new HadoopDistribution("CDH5_2_YARN") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return isYARN(configuration)&&hadoopVersion.toLowerCase().contains("2.5.0-cdh5.2.1");
            }
        });
		
		_hadoopDistributions.add(new HadoopDistribution("HDP2_1_YARN") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return isYARN(configuration)&&hadoopVersion.startsWith("2.4.0.2.1");
            }
        });

        _hadoopDistributions.add(new HadoopDistribution("HDP2_2_YARN") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return isYARN(configuration)&&hadoopVersion.startsWith("2.6.0.2.2");
            }
        });

        _hadoopDistributions.add(new HadoopDistribution("1x") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return hadoopVersion.startsWith("1.2");
            }
        });

        _hadoopDistributions.add(new HadoopDistribution("HADOOP2_YARN") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                // remove isYARN check to support ScaleOut hServer deployments that don't have Hadoop installed.
                return hadoopVersion.startsWith("2.4.1");
            }
        });
		
		_hadoopDistributions.add(new HadoopDistribution("IBM_BI") {
            @Override
            boolean check(String hadoopVersion, Configuration configuration) {
                return !isYARN(configuration)&&hadoopVersion.equals("2.2.0");
            }
        });
    }

    public abstract String getHadoopLibraryString();

    public static HadoopVersionSpecificCode getInstance(String hadoopVersion, Configuration configuration) {
        HadoopDistribution hadoopDistribution = null;
        for (HadoopDistribution candidate : _hadoopDistributions) {
            if (candidate.check(hadoopVersion, configuration)) {
                hadoopDistribution = candidate;
                break;
            }
        }

        if (hadoopDistribution == null) {
            throw new RuntimeException("Hadoop distribution not supported:" + hadoopVersion);
        }

        String localVersion = org.apache.hadoop.util.VersionInfo.getVersion();

        if (!hadoopDistribution.check(localVersion, configuration)) {
            throw new RuntimeException("Hadoop JAR version on the client does not match the version on the server. Client = " + hadoopVersion + "; Server = " + localVersion);
        }

        try {
            return (HadoopVersionSpecificCode) Class.forName(hadoopDistribution.getClassName()).getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate version specific code holder.", e);
        }
    }


    public abstract TaskAttemptID createTaskAttemptId(JobID jobID, boolean isMapper, int hadoopPartition);

    public abstract TaskAttemptContext createTaskAttemptContext(Configuration configuration, TaskAttemptID id);


    public abstract org.apache.hadoop.mapred.JobContext createJobContext(JobConf configuration, JobID jobID);


    public abstract <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Reducer.Context getReducerContext(Configuration configuration,
                                                                                         TaskAttemptID id,
                                                                                         OutputCommitter outputCommitter,
                                                                                         RecordWriter<KEYOUT, VALUEOUT> output,
                                                                                         KeyValueProducer<KEYIN, Iterable<VALUEIN>> transport,
                                                                                         MapOutputAccumulator<KEYOUT, VALUEOUT> mapOutputAccumulatorCallback

    ) throws IOException, InterruptedException;


    public abstract <INKEY, INVALUE, OUTKEY, OUTVALUE> Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context getMapperContext(Configuration configuration, TaskAttemptID taskid, RecordReader reader, RecordWriter writer) throws IOException, InterruptedException;

    public abstract org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContextMapred(JobConf jobConf, org.apache.hadoop.mapred.TaskAttemptID taskId);

    //This method is called once on each worker before starting the job
    public void onJobInitialize(InvocationParameters parameters) throws IOException
    {
    }

    //This method is called when the worker is done with map and reduce phases of the job
    public void onJobDone(InvocationParameters parameters) throws IOException
    {
    }

}
