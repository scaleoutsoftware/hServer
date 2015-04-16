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


import com.scaleoutsoftware.soss.client.da.StateServerException;
import com.scaleoutsoftware.soss.hserver.interop.BucketId;
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for the input formats that read the data stored in the in-memory
 * grid. The data can be stored in the NamedMap or NamedCache.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class GridInputFormat<K, V> extends InputFormat<K, V> {
    protected static final String inputAppIdProperty = "mapred.hserver.input.appId";
    private static final String inputNumberOfSplitsProperty = "mapred.hserver.input.numsplits";

    private static final int DEFAULT_NUMBER_OF_SPLITS = 5;
    private static final int HSERVER_JOB_DEFAULT_NUMBER_OF_SPLITS = 1024;

    /**
     * Sets the desired number of input splits. If the number of splits is not set through
     * this method it will default to the number of available slots in the cluster.
     *
     * @param job            job to modify
     * @param numberOfSplits desired number of split.
     */
    public static void setSuggestedNumberOfSplits(Job job, int numberOfSplits) {
        if (numberOfSplits < 1) {
            throw new IllegalArgumentException("Number of splits should be greater than 0.");
        }
        job.getConfiguration().setInt(inputNumberOfSplitsProperty, numberOfSplits);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        int appId = jobContext.getConfiguration().getInt(inputAppIdProperty, 0);
        int suggestedNumberOfSplits = getSuggestedNumberOfSplits(jobContext);
        return getSplits(appId, suggestedNumberOfSplits);
    }


    static List<InputSplit> getSplits(int appId, int suggestedNumberOfSplits) throws IOException, InterruptedException {
        if (appId == 0) {
            throw new IOException("Input map or cache is not specified.");
        }
        List<BucketId> bucketIds;
        bucketIds = BucketStore.bucketizeNamedCache(appId);


        //Sort buckets by their locations
        Map<InetAddress, List<WritableBucketId>> bucketsByHost = new HashMap<InetAddress, List<WritableBucketId>>();
        WritableBucketId writableId;

        for (BucketId id : bucketIds) {
            writableId = WritableBucketId.copy(id);
            InetAddress location;
            try {
                location = BucketStore.getBucketLocation(id);
            } catch (StateServerException e) {
                throw new IOException("Cannot determine bucket location.", e);
            }
            if (!bucketsByHost.containsKey(location)) {
                bucketsByHost.put(location, new ArrayList<WritableBucketId>());
            }
            bucketsByHost.get(location).add(writableId);
        }


        int numberOfHosts = bucketsByHost.size();

        List<InputSplit> splits = new ArrayList<InputSplit>(suggestedNumberOfSplits);

        int numberOfSplitsPerLocation = Math.max(Math.round(suggestedNumberOfSplits / numberOfHosts), 1);  //We want to have at least one split per location to achieve reasonable parallelism.
        for (InetAddress location : bucketsByHost.keySet()) {  //We go through hosts, distributing each host buckets into several splits
            List<WritableBucketId> bucketIdsForThisHost = bucketsByHost.get(location);
            int numberOfBucketsForThisHost = bucketIdsForThisHost.size();
            int bucketsPerSplit = Math.max(Math.round((float) numberOfBucketsForThisHost / (float) numberOfSplitsPerLocation), 1);

            List<WritableBucketId> bucketIdsForSplit = new ArrayList<WritableBucketId>(bucketsPerSplit);
            int bucketsInTheSplitCounter = 0;
            for (WritableBucketId id : bucketIdsForThisHost) {
                bucketIdsForSplit.add(id);
                bucketsInTheSplitCounter++;
                if (bucketsInTheSplitCounter == bucketsPerSplit) {
                    //Add completed split
                    splits.add(new BucketSplit(bucketIdsForSplit, location));
                    //Start a new split
                    bucketsInTheSplitCounter = 0;
                    bucketIdsForSplit = new ArrayList<WritableBucketId>(bucketsPerSplit);
                }
            }
            if (bucketIdsForSplit.size() != 0) { //Add the remaining buckets
                splits.add(new BucketSplit(bucketIdsForSplit, location));
            }

        }

        //For sanity check, count the number of bucket ids in the split
        int totalNumberOfBucketsInSplits = 0;
        for (InputSplit split : splits) {
            totalNumberOfBucketsInSplits += ((BucketSplit) split).getBucketIds().size();
        }

        if (totalNumberOfBucketsInSplits != bucketIds.size()) //Sanity check
        {
            throw new RuntimeException("Error while calculating splits. Splits contain = " + totalNumberOfBucketsInSplits + ", total buckets = " + bucketIds.size());
        }

        return splits;
    }

    /**
     * Gets the number of input splits. First, tries the corresponding property,
     * then falls back to the number of available slots.
     *
     * @param context job context
     * @return number of input splits
     */
    private int getSuggestedNumberOfSplits(JobContext context) throws IOException {
        int numberOfSplits;
        Configuration conf = context.getConfiguration();
        numberOfSplits = conf.getInt(inputNumberOfSplitsProperty, -1);
        if (numberOfSplits > 0) return numberOfSplits;
        if (HServerParameters.isHServerJob(context.getConfiguration())) { //We are running a hServer job, not a Hadoop job
            return HSERVER_JOB_DEFAULT_NUMBER_OF_SPLITS;
        }
        try {
            ClusterStatus status = (new JobClient((JobConf) context.getConfiguration())).getClusterStatus();
            numberOfSplits = status.getMaxMapTasks() - status.getMapTasks();
            if (numberOfSplits > 0) return numberOfSplits;
        } catch (Throwable t) {
            //Do nothing, will fall back to default;
        }
        return DEFAULT_NUMBER_OF_SPLITS;
    }

}
