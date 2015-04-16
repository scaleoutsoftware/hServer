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


import com.scaleoutsoftware.soss.client.InvocationGrid;
import com.scaleoutsoftware.soss.client.NamedCacheException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * An entry point for running Hadoop MR jobs which use old "mapred" API.
 */
public class HServerJobClient extends JobClient {
    private InvocationGrid grid;
    private boolean unloadGrid = false;
    private String jarPath = null;
    private boolean sortEnabled = false;
    private org.apache.hadoop.mapreduce.JobID jobID;
    private final JobConf jobConf;


    public HServerJobClient(JobConf conf) throws IOException {
        //super(conf);  -- do not attempt to initialize cluster
        jobID = JobID.forName("job_"+conf.getJobName()+"_0");
        jobConf = conf;
    }

    /**
     * Sets the invocation grid used to run the job.
     *
     * @param grid the {@link InvocationGrid} to run the job
     */
    public void setGrid(InvocationGrid grid) {
        this.grid = grid;
    }

    /**
     * Sets the path to the JAR which contains the job classes.
     *
     * @param jarPath path to the JAR file
     */
    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    /**
     * Enables/disables sorting of the keys within each partition.
     * Sorting is turned off by default.
     *
     * @param sortEnabled <code>true</code> if sorting is enabled,
     *                    <code>false</code> otherwise
     */
    public void setSortEnabled(boolean sortEnabled) {
        this.sortEnabled = sortEnabled;
    }


    /**
     * Gets whether sorting of the the keys within each partition is enabled.
     *
     * @return <code>true</code> if sorting is enabled
     */
    boolean getSortEnabled() {
        return sortEnabled;
    }

    @Override
    public RunningJob submitJob(final JobConf job) throws IOException
    {
        ensureInvocationGridPresent();
        ExecutorService async = Executors.newSingleThreadExecutor();
        final JobID jobID = JobID.forName("job_"+job.getJobName()+"_0");

        Future jobSubmitted = async.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    JobScheduler.getInstance().runOldApiJob(job, jobID, sortEnabled, null, grid);
                } finally {
                    if (unloadGrid) {
                        grid.unload();
                    }
                }
                return null;
            }
        });
        async.shutdown(); //Will shut down after task is done

        return new HServerRunningJob(jobID, jobSubmitted);
    }

    /**
     * Checks if the invocation grid is in place and starts one if necessary.
     */
    private void ensureInvocationGridPresent() throws IOException {
        if (grid == null) {
            try {
                String jar = (jarPath == null ? jobConf.getJar() : jarPath);
                if (jar == null) {
                    throw new IOException("Cannot identify job JAR. Use HServerJobClient.setJarPath()/JobConf.setJarByClass() or specify invocation grid.");
                }
                grid = HServerJob.getInvocationGridBuilder("HServerIG-" + jobID+"_"+ UUID.randomUUID()).
                        addJar(jar).
                        setLingerTimeMinutes(60).
                        load();
                unloadGrid = true;
            } catch (NamedCacheException e) {
                throw new IOException("Cannot load the invocation grid.", e);
            }
        }
    }


    /**
     * Submit the job for execution.
     *
     * @param job job for the execution
     * @param sortEnabled enable sorting of reduce keys for that job
     *
     * @return {@link RunningJob} handler
     * @throws IOException if a ScaleOut hServer access error occurred
     */
    public static RunningJob runJob(final JobConf job, boolean sortEnabled) throws IOException {
        HServerJobClient jobClient = new HServerJobClient(job);
        jobClient.setSortEnabled(sortEnabled);
        RunningJob hServerRunningJob = jobClient.submitJob(job);
        hServerRunningJob.waitForCompletion();
        return hServerRunningJob;
    }

    /**
     * Submit the job for execution.
     *
     * @param job job for the execution
     * @param sortEnabled enable sorting of reduce keys for that job
     * @param grid invocation grid to run job on
     *
     * @return {@link RunningJob} handler
     * @throws IOException if a ScaleOut hServer access error occurred
     */
    public static RunningJob runJob(final JobConf job,  boolean sortEnabled, final InvocationGrid grid) throws IOException {
        HServerJobClient jobClient = new HServerJobClient(job);
        jobClient.setSortEnabled(sortEnabled);
        jobClient.setGrid(grid);
        jobClient.unloadGrid = false;
        RunningJob hServerRunningJob = jobClient.submitJob(job);
        hServerRunningJob.waitForCompletion();
        return hServerRunningJob;
    }

    /**
     * Submit the job for execution.
     *
     * @param job job for the execution
     *
     * @return {@link RunningJob} handler
     * @throws IOException if a ScaleOut hServer access error occurred
     */
    public static RunningJob runJob(JobConf job) throws IOException {
        HServerJobClient jobClient = new HServerJobClient(job);
        RunningJob hServerRunningJob = jobClient.submitJob(job);
        hServerRunningJob.waitForCompletion();
        return hServerRunningJob;
    }

    static class HServerRunningJob implements RunningJob {
        JobID jobID;
        Future runningJob;

        HServerRunningJob(JobID jobID, Future runningJob) {
            this.jobID = jobID;
            this.runningJob = runningJob;
        }

        @Override
        public JobID getID() {
            return jobID;
        }

        @Override
        public String getJobID() {
            return jobID.toString();
        }

        @Override
        public String getJobName() {
            return jobID.toString();
        }

        @Override
        public String getJobFile() {
            return "";
        }

        @Override
        public String getTrackingURL() {
            return "";
        }

        @Override
        public float mapProgress() throws IOException {
            return isComplete() ? 1.0f : 0.0f;
        }

        @Override
        public float reduceProgress() throws IOException {
            return isComplete() ? 1.0f : 0.0f;
        }

        @Override
        public float cleanupProgress() throws IOException {
            return isComplete() ? 1.0f : 0.0f;
        }

        @Override
        public float setupProgress() throws IOException {
            return isComplete() ? 1.0f : 0.0f;
        }

        @Override
        public boolean isComplete() throws IOException {
            return runningJob.isDone();
        }

        @Override
        public boolean isSuccessful() throws IOException {
            return runningJob.isDone();
        }

        @Override
        public void waitForCompletion() throws IOException {
            try {
                runningJob.get();
            } catch (Exception e) {
                throw new IOException("Job failed.", e);
            }
        }

        @Override
        public int getJobState() throws IOException {
            return isComplete()?JobStatus.SUCCEEDED:JobStatus.FAILED;
        }

        @Override
        public JobStatus getJobStatus() throws IOException {
            return new JobStatus(jobID, setupProgress(), mapProgress(), reduceProgress(), cleanupProgress(), getJobState(), JobPriority.NORMAL);
        }

        @Override
        public void killJob() throws IOException {
            runningJob.cancel(true);
        }

        @Override
        public void setJobPriority(String s) throws IOException {
            //Do nothing
        }

        @Override
        public TaskCompletionEvent[] getTaskCompletionEvents(int i) throws IOException {
            return null;
        }

        @Override
        public void killTask(TaskAttemptID taskAttemptID, boolean b) throws IOException {
            //Do nothing
        }

        @Override
        public void killTask(String s, boolean b) throws IOException {
            //Do nothing
        }

        @Override
        public Counters getCounters() throws IOException {
            return null;
        }

        @Override
        public String getFailureInfo() throws IOException {
            return null;
        }

        @Override
        public String[] getTaskDiagnostics(TaskAttemptID taskAttemptID) throws IOException {
            return new String[0];
        }

        //@Override
        public Configuration getConfiguration() {
            return null;
        }

        //@Override
        public String getHistoryUrl() throws IOException {
            return "";
        }

       //@Override
        public boolean isRetired() throws IOException {
            return false;
        }
    }

}
