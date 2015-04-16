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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unchecked")
public class HServerClientProtocol implements ClientProtocol {
    private static final AtomicInteger jobIdCounter = new AtomicInteger(0);
    private static final String applicationId = "hserver" + (new Random()).nextInt();

    private static final Log LOG = LogFactory.getLog(HServerClientProtocol.class);

    private static ConcurrentMap<org.apache.hadoop.mapred.JobID, SubmittedJob> submittedJobs = new ConcurrentHashMap<org.apache.hadoop.mapred.JobID, SubmittedJob>();

    private Configuration configuration;

    private YARNRunner backupRunner;


    public HServerClientProtocol(Configuration configuration) {
        this.configuration = configuration;
        backupRunner = new YARNRunner(configuration);
    }

    @Override
    public JobID getNewJobID() throws IOException, InterruptedException {
        return new org.apache.hadoop.mapreduce.JobID(applicationId, jobIdCounter.incrementAndGet());
    }

    @Override
    public JobStatus submitJob(final JobID jobID, String jobSubmitDirectory, Credentials credentials) throws IOException, InterruptedException {
        SubmittedJob job = new SubmittedJob(jobID, jobSubmitDirectory, credentials, configuration);
        job.submit();
        submittedJobs.put(org.apache.hadoop.mapred.JobID.downgrade(jobID), job);
        return job.getJobStatus();
    }


    @Override
    public ClusterMetrics getClusterMetrics() throws IOException, InterruptedException {
        return backupRunner.getClusterMetrics();
    }

    @Override
    public Cluster.JobTrackerStatus getJobTrackerStatus() throws IOException, InterruptedException {
        return backupRunner.getJobTrackerStatus();
    }

    @Override
    public long getTaskTrackerExpiryInterval() throws IOException, InterruptedException {
        return backupRunner.getTaskTrackerExpiryInterval();
    }

    @Override
    public AccessControlList getQueueAdmins(String s) throws IOException {
        return backupRunner.getQueueAdmins(s);
    }

    @Override
    public void killJob(JobID jobID) throws IOException, InterruptedException {
        if (submittedJobs.containsKey(org.apache.hadoop.mapred.JobID.downgrade(jobID))) {
            submittedJobs.get(org.apache.hadoop.mapred.JobID.downgrade(jobID)).cancel();
        } else {
            backupRunner.killJob(jobID);
        }
    }

    @Override
    public void setJobPriority(JobID jobID, String s) throws IOException, InterruptedException {
        if (submittedJobs.containsKey(org.apache.hadoop.mapred.JobID.downgrade(jobID))) {
            //Do nothing
        } else {
            backupRunner.setJobPriority(jobID, s);
        }
    }

    @Override
    public boolean killTask(TaskAttemptID taskAttemptID, boolean b) throws IOException, InterruptedException {
        return backupRunner.killTask(taskAttemptID, b);
    }

    @Override
    public JobStatus getJobStatus(JobID jobID) throws IOException, InterruptedException {
        SubmittedJob job = submittedJobs.get(org.apache.hadoop.mapred.JobID.downgrade(jobID));
        if (job == null) {
            return backupRunner.getJobStatus(jobID);
        } else {
            return job.getJobStatus();
        }
    }

    @Override
    public Counters getJobCounters(JobID jobID) throws IOException, InterruptedException {
        if (submittedJobs.containsKey(org.apache.hadoop.mapred.JobID.downgrade(jobID))) {
            return null;
        }
        return backupRunner.getJobCounters(jobID);
    }

    @Override
    public TaskReport[] getTaskReports(JobID jobID, TaskType taskType) throws IOException, InterruptedException {
        if (submittedJobs.containsKey(org.apache.hadoop.mapred.JobID.downgrade(jobID))) {
            return new TaskReport[0];
        }
        return backupRunner.getTaskReports(jobID, taskType);
    }

    @Override
    public String getFilesystemName() throws IOException, InterruptedException {
        return backupRunner.getFilesystemName();
    }

    @Override
    public JobStatus[] getAllJobs() throws IOException, InterruptedException {
        return backupRunner.getAllJobs();
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobID, int i, int i2) throws IOException, InterruptedException {
        if (submittedJobs.containsKey(org.apache.hadoop.mapred.JobID.downgrade(jobID))) {
            return new TaskCompletionEvent[0];
        } else {
            return backupRunner.getTaskCompletionEvents(jobID, i, i2);
        }
    }

    @Override
    public String[] getTaskDiagnostics(TaskAttemptID taskAttemptID) throws IOException, InterruptedException {
        return backupRunner.getTaskDiagnostics(taskAttemptID);
    }

    @Override
    public TaskTrackerInfo[] getActiveTrackers() throws IOException, InterruptedException {
        return backupRunner.getActiveTrackers();
    }

    @Override
    public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException, InterruptedException {
        return backupRunner.getBlacklistedTrackers();
    }

    @Override
    public String getSystemDir() throws IOException, InterruptedException {
        return backupRunner.getSystemDir();
    }

    @Override
    public String getStagingAreaDir() throws IOException, InterruptedException {
        return backupRunner.getStagingAreaDir();
    }

    @Override
    public String getJobHistoryDir() throws IOException, InterruptedException {
        return backupRunner.getJobHistoryDir();
    }

    @Override
    public QueueInfo[] getQueues() throws IOException, InterruptedException {
        return backupRunner.getQueues();
    }

    @Override
    public QueueInfo getQueue(String s) throws IOException, InterruptedException {
        return backupRunner.getQueue(s);
    }

    @Override
    public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException, InterruptedException {
        return backupRunner.getQueueAclsForCurrentUser();
    }

    @Override
    public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
        return backupRunner.getRootQueues();
    }

    @Override
    public QueueInfo[] getChildQueues(String s) throws IOException, InterruptedException {
        return backupRunner.getChildQueues(s);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text text) throws IOException, InterruptedException {
        return backupRunner.getDelegationToken(text);
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> delegationTokenIdentifierToken) throws IOException, InterruptedException {
        return backupRunner.renewDelegationToken(delegationTokenIdentifierToken);
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> delegationTokenIdentifierToken) throws IOException, InterruptedException {
        backupRunner.cancelDelegationToken(delegationTokenIdentifierToken);
    }

    @Override
    public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID) throws IOException, InterruptedException {
        return backupRunner.getLogFileParams(jobID, taskAttemptID);
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return backupRunner.getProtocolVersion(s, l);
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return backupRunner.getProtocolSignature(s, l, i);
    }
}
