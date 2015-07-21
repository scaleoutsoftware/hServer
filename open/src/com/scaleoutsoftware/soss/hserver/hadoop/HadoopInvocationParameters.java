/*
 Copyright (c) 2013 by ScaleOut Software, Inc.

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


import com.scaleoutsoftware.soss.client.pmi.InvocationGridLocalCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * This class is a wrapper over Hadoop specific invocation parameters.
 * It is passed to com.scaleoutsoftware.soss.hserver.InvocationParameters to make it Hadoop-agnostic;
 */
public class HadoopInvocationParameters {
    Configuration configuration;
    org.apache.hadoop.mapreduce.JobID jobID;
    private Class<?> configurationClass;
    boolean isOldApi = false;

    /**
     * This constructor is used to create the instance on the ivoking side
     */
    public HadoopInvocationParameters(Configuration configuration,  org.apache.hadoop.mapreduce.JobID jobID, boolean isOldApi) {
        this.configuration = configuration;
        this.jobID = jobID;
        this.configurationClass = configuration.getClass();
        this.isOldApi = isOldApi;
    }

    /**
     * This constructor is used during deserialization
     */
    public HadoopInvocationParameters(java.io.ObjectInputStream instream) throws IOException, ClassNotFoundException {
        isOldApi = instream.readBoolean();
        configurationClass = (Class) instream.readObject();
        jobID = isOldApi ? new org.apache.hadoop.mapred.JobID() : new org.apache.hadoop.mapreduce.JobID();
        configuration = (Configuration) ReflectionUtils.newInstance(configurationClass, null);
        configuration.readFields(instream);
        configuration.setClassLoader(InvocationGridLocalCache.getClassLoader());
        jobID.readFields(instream);
    }

    public void serialize(ObjectOutputStream out) throws IOException {
        out.writeBoolean(isOldApi);
        out.writeObject(configurationClass);
        configuration.write(out);
        jobID.write(out);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public org.apache.hadoop.mapreduce.JobID getJobID() {
        return jobID;
    }

    public boolean isOldApi() {
        return isOldApi;
    }

    public static String dumpConfiguration(Object conf) {
        if (conf instanceof Configuration) {

            StringBuilder builder = new StringBuilder();
            builder.append("{ ");
            for (Map.Entry<String, String> entry : (Configuration) conf) {
                builder.append("{");
                builder.append(entry.getKey());
                builder.append(",");
                builder.append(entry.getValue());
                builder.append("} ");
            }
            builder.append("}");
            return builder.toString();
        } else {
            return conf.toString();
        }
    }

    @Override
    public String toString() {
        return "HadoopInvocationParameters{" +
                "configuration=" + dumpConfiguration(configuration) +
                ", jobID=" + jobID +
                ", configurationClass=" + configurationClass +
                ", isOldApi=" + isOldApi +
                '}';
    }
}
