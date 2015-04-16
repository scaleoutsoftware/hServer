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
import com.scaleoutsoftware.soss.client.InvocationGridBuilder;
import com.scaleoutsoftware.soss.client.NamedCacheException;
import com.scaleoutsoftware.soss.client.util.BitConverter;
import com.scaleoutsoftware.soss.hserver.hadoop.HadoopVersionSpecificCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

import static com.scaleoutsoftware.soss.hserver.interop.HServerConstants.MAX_MAP_REDUCE_TASKS;

/**
 * This class should should be used to direct the map reduce application to
 * use ScaleOut hServer infrastructure instead of a job tracker/task trackers.
 * It should be used in place of a {@link Job} class to make a Hadoop application
 * run on ScaleOut hServer.
 */
public class HServerJob extends Job {
    private JobID jobID;
    private InvocationGrid grid;
    private boolean sortEnabled = false;
    private Future jobSubmitted = null;
    private boolean unloadGrid = false;
    private String jarPath = null;
    private Object jobParameter;

    private enum HServerJobState {Define, Running, Failed, Succeded}


    private HServerJobState jobState = HServerJobState.Define;


    /**
     * Constructs the ScaleOut hServer job object. Sorting of the
     * reducer input keys is turned on by default. A new invocation
     * grid containing the JAR for the job will be constructed
     * each time the job is run.
     *
     * @param conf configuration
     * @throws IOException if the job cannot be constructed
     */
    public HServerJob(Configuration conf) throws IOException {
        this(conf, "HServerJob");
    }


    /**
     * Constructs the ScaleOut hServer job object. Sorting of the
     * reducer input keys is turned on by default. A new invocation
     * grid containing the JAR for the job will be constructed
     * each time the job is run.
     *
     * @param conf    configuration
     * @param jobName job name
     * @throws IOException if the job cannot be constructed
     */
    public HServerJob(Configuration conf, String jobName) throws IOException {
        this(conf, jobName, true, null);
    }

    /**
     * Constructs the ScaleOut hServer job object. A new invocation
     * grid containing the JAR for the job will be constructed
     * each time the job is run.
     *
     * @param conf        configuration
     * @param jobName     job name
     * @param sortEnabled <code>true</code> if the reducer input key should be
     *                    sorted within each partition, or <code>false</code>
     *                    if sorting is not necessary.
     * @throws IOException if the job cannot be constructed
     */
    public HServerJob(Configuration conf, String jobName, boolean sortEnabled) throws IOException {
        this(conf, jobName, sortEnabled, null);
    }

    /**
     * Constructs the ScaleOut hServer job object.
     *
     * @param conf        configuration
     * @param jobName     job name
     * @param sortEnabled <code>true</code> if the reducer input key should be
     *                    sorted within each partition, or <code>false</code>
     *                    if sorting is not necessary.
     * @param grid        {@link InvocationGrid} to be used for running the job
     * @throws IOException if the job cannot be constructed
     */
    public HServerJob(Configuration conf, String jobName, boolean sortEnabled, InvocationGrid grid) throws IOException {
        super(conf, jobName);
        this.sortEnabled = sortEnabled;
        this.grid = grid;
        //We do not have a way to keep track of the number of jobs performed, so make jobtracker id
        //param unique instead by appending a UUID
        jobID = new JobID("HServer_" + jobName + "_" + UUID.randomUUID(), 0);
        this.getConfiguration().setBoolean(HServerParameters.IS_HSERVER_JOB, true);
        this.getConfiguration().setInt(HServerParameters.INVOCATION_ID, getAppId());
    }

    /**
     * Runs a single result optimisation and returns the result object.
     *
     * @return result object
     * @throws IOException if errors occurred during the job
     * @throws InterruptedException if the processing thread is interrupted
     * @throws ClassNotFoundException if the invocation grid does not contain the dependency class
     */
    public Object runAndGetResult() throws IOException, InterruptedException, ClassNotFoundException {
        ensureInvocationGridPresent();
        return JobScheduler.getInstance().runOptimisation(this, grid);
    }

    @Override
    public boolean isComplete() throws IOException {
        return jobState == HServerJobState.Succeded || jobState == HServerJobState.Failed;
    }

    @Override
    public boolean isSuccessful() throws IOException {
        return jobState == HServerJobState.Succeded;
    }

    @Override
    public float setupProgress() throws IOException {
        return isSuccessful() ? 1.0f : 0.0f;
    }

    @Override
    public float reduceProgress() throws IOException {
        return isSuccessful() ? 1.0f : 0.0f;
    }

    @Override
    public float mapProgress() throws IOException {
        return isSuccessful() ? 1.0f : 0.0f;
    }

    @Override
    public void killJob() throws IOException {
        if (jobSubmitted != null) {
            jobSubmitted.cancel(true);
            jobState = HServerJobState.Failed;
        }
    }

    /**
     * Submits the job to the ScaleOut hServer and waits for it to finish.
     *
     * @param verbose print the progress to the user
     * @return true if the job succeeded. This job always returns <code>true</code>
     *         or throws an exception if the job failed for some reason.
     * @throws IOException thrown if the errors occurred during the job
     */
    @Override
    public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
        submit();
        try {
            jobSubmitted.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            jobState = HServerJobState.Failed;
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof ClassNotFoundException) {
                throw (ClassNotFoundException) cause;
            } else {
                throw new RuntimeException(e);
            }
        }
        return true;
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
    /**
     * Gets the job ID for the job.
     */
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public void setNumReduceTasks(int tasks) throws IllegalStateException {
        //We do not support more reduce tasks than  partitions.
        super.setNumReduceTasks(Math.min(tasks, MAX_MAP_REDUCE_TASKS));
    }

    /**
     * Gets the SOSS appID which is used to store temporary objects for the job.
     *
     * @return appId value
     */
    int getAppId() {
        return 0xFFFFFFF & BitConverter.hashStringOneInt(jobID.toString());
    }

    /**
     * Adds a parameter object to the job. This parameter object is sent out
     * to all worker nodes and can be retrieved by {@link JobParameter#get(org.apache.hadoop.conf.Configuration)}.
     *
     * @param jobParameter user defined parameter object
     */
    public void setJobParameter(Object jobParameter) {
        this.jobParameter = jobParameter;
    }

    Object getJobParameter() {
        return jobParameter;
    }

    @Override
    /**
     * Asynchronously submits the job for execution to the ScaleOut hServer.
     */
    public void submit() throws IOException, InterruptedException, ClassNotFoundException {
        ensureInvocationGridPresent();
        ExecutorService async = Executors.newSingleThreadExecutor();
        final HServerJob job = this;
        jobSubmitted = async.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    JobScheduler.getInstance().runJob(job, grid);
                } catch (Exception e) {
                    jobState = HServerJobState.Failed;
                    throw e;
                } finally {
                    if (job.unloadGrid) {
                        grid.unload();
                    }
                }
                jobState = HServerJobState.Succeded;
                return null;
            }
        });
        jobState = HServerJobState.Running;
        async.shutdown();
    }

    /**
     * Checks if the invocation grid is in place and starts one if necessary.
     */
    private void ensureInvocationGridPresent() throws IOException {
        if (grid == null) {
            try {
                String jar = (jarPath == null ? this.getJar() : jarPath);
                if (jar == null) {
                    throw new IOException("Cannot identify job JAR. Use HServerJob.setJarPath()/setJarByClass() or specify invocation grid.");
                }
                grid = getInvocationGridBuilder("HServerIG-" + jobID, conf).
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
     * Creates an {@link InvocationGridBuilder} and configures it with hServer system
     * settings. If the invocation grid is intended to be compatible with ScaleOut hServer, this method
     * should be used to create it instead of {@link InvocationGridBuilder#InvocationGridBuilder(String)}.
     *
     * @param name name of the invocation grid
     * @param configuration configuration
     * @return builder for the invocation grid
     * @throws NamedCacheException if ScaleOut StateServer is unavailable.
     */
    public static InvocationGridBuilder getInvocationGridBuilder(String name, Configuration configuration) throws NamedCacheException {
        String user = "";
        try {
            String username = UserGroupInformation.getCurrentUser().getUserName();
            if (username != null && username.length() != 0) {
                user = " -DHADOOP_USER_NAME=" + UserGroupInformation.getCurrentUser().getUserName();
            }
        } catch (Throwable t) {
            //Do nothing, will start IG without username
        }
        return new InvocationGridBuilder(name).
                setNewClassLoader(false).
                setJVMParameters(user).
                setLibraryPath(HadoopVersionSpecificCode.getInstance(VersionInfo.getVersion(), configuration).getHadoopLibraryString());

    }

    /**
     * Creates an {@link InvocationGridBuilder} and configures it with hServer system
     * settings. If the invocation grid is intended to be compatible with ScaleOut hServer, this method
     * should be used to create it instead of {@link InvocationGridBuilder#InvocationGridBuilder(String)}.
     *
     * @param name name of the invocation grid
     * @return builder for the invocation grid
     * @throws NamedCacheException if ScaleOut StateServer is unavailable.
     */
    public static InvocationGridBuilder getInvocationGridBuilder(String name) throws NamedCacheException {
        return getInvocationGridBuilder(name, new Configuration());
    }


}
