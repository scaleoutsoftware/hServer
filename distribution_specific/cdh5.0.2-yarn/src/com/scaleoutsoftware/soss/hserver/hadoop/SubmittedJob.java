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

import com.scaleoutsoftware.soss.client.InvocationGrid;
import com.scaleoutsoftware.soss.client.InvocationGridBuilder;
import com.scaleoutsoftware.soss.hserver.HServerJob;
import com.scaleoutsoftware.soss.hserver.JobScheduler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Job submitted to SOSS hServer from the point of view of the {@link HServerClientProtocol}.
 * It parses the staging directory which was prepared for the YARN runner and
 * calls {@link JobScheduler} on a separate thread to submit the job.
 * The job progress is saved by keeping the {@link Future}.
 */
class SubmittedJob implements Callable {
    private static final Log LOG = LogFactory.getLog(SubmittedJob.class);

    private final static List<String> classLookupCandidateProperties = Arrays.asList(
            "mapred.mapper.class",
            "mapred.reducer.class",
            "mapred.combiner.class",
            "mapreduce.job.map.class",
            "mapreduce.job.reduce.class",
            "mapreduce.job.combine.class");

    private final JobConf jobConf;
    private final JobID jobID;
    private final Configuration configuration;
    private final Path jobSubmitDirectoryPath;
    private final boolean newApi;
    private final List<Object> inputSplits = new ArrayList<Object>();
    private final Map<Object, String[]> splitLocations = new HashMap<Object, String[]>();
    private final FileSystem fileSystem;
    private final Path jobConfPath;

    private static InvocationGrid grid;
    private static final Object _lock = new Object();

    private Future runningJob;
    private JobStatus jobStatus;




    SubmittedJob(JobID jobID, String jobSubmitDirectory, Credentials credentials, Configuration configuration) throws IOException, InterruptedException {
        this.jobID = jobID;
        this.configuration = configuration;
        this.jobSubmitDirectoryPath = new Path(jobSubmitDirectory);
        this.fileSystem = FileSystem.get(configuration);

        JobSplit.TaskSplitMetaInfo splitInfo[] = SplitMetaInfoReader.readSplitMetaInfo(jobID, fileSystem, configuration, jobSubmitDirectoryPath);

        Path jobSplitFile = JobSubmissionFiles.getJobSplitFile(jobSubmitDirectoryPath);
        FSDataInputStream stream = fileSystem.open(jobSplitFile);

        for (JobSplit.TaskSplitMetaInfo info : splitInfo) {
            Object split = getSplitDetails(stream, info.getStartOffset(), configuration);
            inputSplits.add(split);
            splitLocations.put(split, info.getLocations());
            LOG.info("Adding split for execution. Split = " + split + " Locations: " + Arrays.toString(splitLocations.get(split)));
        }

        stream.close();

        jobConfPath = JobSubmissionFiles.getJobConfPath(jobSubmitDirectoryPath);

        if (!fileSystem.exists(jobConfPath)) {
            throw new IOException("Cannot find job.xml. Path = " + jobConfPath);
        }

        //We cannot just use JobConf(Path) constructor,
        //because it does not work for HDFS locations.
        //The comment in Configuration#loadResource() states,
        //for the case when the Path to the resource is provided:
        //"Can't use FileSystem API or we get an infinite loop
        //since FileSystem uses Configuration API.  Use java.io.File instead."
        //
        //Workaround: construct empty Configuration, provide it with
        //input stream and give it to JobConf constructor.
        FSDataInputStream jobConfStream = fileSystem.open(jobConfPath);
        Configuration jobXML = new Configuration(false);
        jobXML.addResource(jobConfStream);

        //The configuration does not actually gets read before we attempt to
        //read some property. Call to #size() will make Configuration to
        //read the input stream.
        jobXML.size();

        //We are done with input stream, can close it now.
        jobConfStream.close();

        jobConf = new JobConf(jobXML);

        newApi = jobConf.getUseNewMapper();


        jobStatus = new JobStatus(jobID, 0f, 0f, 0f, 0f,
                JobStatus.State.RUNNING,
                JobPriority.NORMAL,
                UserGroupInformation.getCurrentUser().getUserName(),
                jobID.toString(),
                jobConfPath.toString(), "");
    }


    @Override
    public Object call() throws Exception {
        try {
            //Only run if we have splits
            if (inputSplits.size() > 0) {
                synchronized (_lock) {
                    if (grid == null) {
                        InvocationGridBuilder gridBuilder = HServerJob.
                                getInvocationGridBuilder("hServerIG"/*jobID.toString()*/, jobConf);  //Reuse IG if it is already loaded

                        if (jobConf.getJar() != null) {
                            //File stream will be closed by IG builder
                            Path defaultJar = new Path(jobConf.getJar());
                            gridBuilder.addJar("job.jar", fileSystem.open(defaultJar));
                            LOG.info("Adding a job jar to the IG. Size = " + fileSystem.getFileStatus(defaultJar).getLen());
                        } else {
                            //If we do not have the main JAR, try adding mapper/reducer/combiner
                            for (String property : classLookupCandidateProperties) {
                                Class clazz = jobConf.getClass(property, null);
                                if (clazz != null && !clazz.getCanonicalName().startsWith("org.apache.hadoop")) {
                                    gridBuilder.addClass(clazz);
                                }
                            }
                        }

                        //Hive case, multiple IG workers
                        LOG.warn("MAPPER = " + jobConf.get("mapred.mapper.class", ""));
                        if (jobConf.get("mapred.mapper.class", "").equals("org.apache.hadoop.hive.ql.exec.mr.ExecMapper")) {
                            LOG.warn("Starting IG in Hive mode (multiple worker JVMs).");
                            gridBuilder.setOneWorkerPerCore(true);
                            jobConf.setInt("mapred.hserver.maxslots", 1);
                        }

                        String javaOpts = jobConf.get("mapred.child.java.opts", "");
                        if (javaOpts.length() > 0) {
                            LOG.warn("Parsed child JVM parameters:" + javaOpts);
                            gridBuilder.setJVMParameters(javaOpts);
                        }

                        //Adding libjars
                        addJarsFromConfigurationProperty(gridBuilder, configuration, "tmpjars");

                        grid = gridBuilder.load();
                    }
                }
                LOG.info("Reusing invocation grid for MR job.");
                JobScheduler.getInstance().runPredefinedJob(jobID, jobConf, newApi, inputSplits.get(0).getClass(), inputSplits, splitLocations, grid);

            }
            return null;
        } finally {
            //We no longer need staging directory
            fileSystem.delete(jobSubmitDirectoryPath, true);
        }
    }

    /**
     * Parses the comma-separated list of jars and adds them to the IG.
     *
     * @param invocationGrid IG builder to add jars to
     * @param configuration  job configuration
     * @param propertyName   name of the property containing jars
     * @return number of jars added to the IG
     */
    public static int addJarsFromConfigurationProperty(InvocationGridBuilder invocationGrid, Configuration configuration, String propertyName) {
        String jars = configuration.get("tmpjars");
        int count = 0;
        if (jars != null) {
            String[] jarNames = jars.split(",");
            for (String jar : jarNames) {
                try {
                    invocationGrid.addJar((new URL(jar)).getPath());
                    LOG.info("JAR " + jar + ", specified in property " + propertyName + " added to the invocation grid.");
                    count++;
                } catch (Exception e) {
                    LOG.warn("Cannot add JAR " + jar + ", specified in property " + propertyName, e);
                }

            }
        }
        return count;
    }


    //Based on getSplitDetails() from MapTask.java in Apache Hadoop 2.2.0
    @SuppressWarnings("unchecked")
    private static <T> T getSplitDetails(FSDataInputStream inFile, long offset, Configuration configuration)
            throws IOException {
        inFile.seek(offset);
        String className = StringInterner.weakIntern(Text.readString(inFile));
        Class<T> cls;
        try {
            cls = (Class<T>) configuration.getClassByName(className);
        } catch (ClassNotFoundException ce) {
            IOException wrap = new IOException("Split class " + className +
                    " not found");
            wrap.initCause(ce);
            throw wrap;
        }
        SerializationFactory factory = new SerializationFactory(configuration);
        Deserializer<T> deserializer =
                (Deserializer<T>) factory.getDeserializer(cls);
        deserializer.open(inFile);
        T split = deserializer.deserialize(null);
        return split;
    }

    JobStatus getJobStatus() throws IOException {
        if (runningJob != null && runningJob.isDone()) {
            try {
                runningJob.get();
                jobStatus = new JobStatus(jobID, 1f, 1f, 1f, 1f,
                        JobStatus.State.SUCCEEDED,
                        JobPriority.NORMAL,
                        UserGroupInformation.getCurrentUser().getUserName(),
                        jobID.toString(),
                        jobConfPath.toString(), "");

            } catch (Exception e) {
                LOG.error("Exception while running ScaleOut hServer job.", e);
                final String failureInfo = e.toString();
                jobStatus = new JobStatus(jobID, 0f, 0f, 0f, 0f,
                        JobStatus.State.FAILED,
                        JobPriority.NORMAL,
                        UserGroupInformation.getCurrentUser().getUserName(),
                        jobID.toString(),
                        jobConfPath.toString(), "") {
                    @Override
                    public synchronized String getFailureInfo() {
                        return failureInfo;
                    }
                };
            }
            runningJob = null;
        }
        return jobStatus;
    }

    public void submit() {
        ExecutorService async = Executors.newSingleThreadExecutor();
        runningJob = async.submit(this);
        async.shutdown();
    }

    public void cancel() {
        if (runningJob != null) {
            runningJob.cancel(true);
        }
    }

}
