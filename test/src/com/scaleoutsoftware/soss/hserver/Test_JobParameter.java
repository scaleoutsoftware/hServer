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
import com.scaleoutsoftware.soss.client.map.BulkLoader;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;

/**
 * Test that checks if a user-defined custom object can be asynchronously passed to the mappers and reducers.
 */
public class Test_JobParameter {
    private final static String JOB_PARAMETER = "job parameter";
    public static class WordCountMapper extends Mapper<Integer, String, String, Integer> {
        private final static IntWritable one = new IntWritable(1);
        int from = 0;
        int to = 0;
        int length = 0;
        final int SPACE = 0x20;
        byte buf[];
        private Text word = new Text();

        /*
         * Optimized map function that doesn't use "throw away objects". Operates by analyzing an array of bytes and
         * finds words by cutting the array from the start of the word to the next space character(0x20 - in utf8).
         */
        public void map(Integer key, String value, Context context) throws IOException, InterruptedException {
            if(!JOB_PARAMETER.equals(JobParameter.get(context.getConfiguration())))
            {
                throw new RuntimeException("Job parmeter passing failed");
            }
            buf = value.getBytes();
            length = value.length();
            from = 0;
            while (true) {
                for (to = from; to < length; to++) {
                    if (buf[to] == SPACE) break;
                }
                word.set(buf, from, to - from);
                context.write(word.toString(), one.get());
                from = to + 1;
                if (from >= length) return;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class WordCountReducer extends Reducer<String, Integer, String, Integer> {

        public WordCountReducer() {
        }

        @Override
        public void reduce(String key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
            if(!JOB_PARAMETER.equals(JobParameter.get(context.getConfiguration())))
            {
                throw new RuntimeException("Job parmeter passing failed");
            }
            int sum = 0;
            for (Integer val : values) {
                sum += val;
            }
            context.write(key,  sum);
        }
    }

    public static class Combiner extends Reducer<String, Integer, String, Integer> {
        public Combiner() {
        }

        @Override
        public void reduce(String key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
            if(!JOB_PARAMETER.equals(JobParameter.get(context.getConfiguration())))
            {
                throw new RuntimeException("Job parmeter passing failed");
            }
            int sum = 0;
            for (Integer val : values) {
                sum += val;
            }
            context.write(key, sum);
        }
    }

    public static void main(String argv[]) throws Exception {
        InvocationGrid grid = HServerJob.getInvocationGridBuilder("MyGrid" + System.currentTimeMillis())
                .addJar("/path/to/your/classes.jar")
                .addClass(Test_JobParameter.class)
                .setLibraryPath("hadoop-1.2.1")
                .load();


        // Create input and output map
        NamedMap<Integer, String> inputMap = NamedMapFactory.getMap("inputMap");
        NamedMap<String, Integer> outputMap = NamedMapFactory.getMap("outputMap");
        inputMap.clear();
        outputMap.clear();

        // Puts 100k strings into the input map
        int numstrings = argv.length == 1 ? new Integer(argv[0]) : 1000;
        BulkLoader<Integer, String> put = inputMap.getBulkLoader();
        for (int i = 0; i < numstrings; i++) {
            put.put(i, i % 2 == 0 ? "The quick brown fox jumps over the lazy dog" : "Few black taxis drive up major roads on quiet hazy nights");
        }
        put.close();

        // Sets up the Hadoop job, which uses GridInputFormat and GridOutputFormat
        Configuration conf = new Configuration();
        HServerJob job = new HServerJob(conf, "Sample job"+System.currentTimeMillis());
        job.setGrid(grid);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(Combiner.class);
        job.setMapOutputKeyClass(String.class);
        job.setMapOutputValueClass(Integer.class);
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Integer.class);

        job.setInputFormatClass(NamedMapInputFormat.class);
        job.setOutputFormatClass(GridOutputFormat.class);
        job.setJobParameter(JOB_PARAMETER);

        // Setting the input format properties: input object class and input cache
        NamedMapInputFormat.setNamedMap(job, inputMap);

        // Setting the output map
        GridOutputFormat.setNamedMap(job, outputMap);
        job.waitForCompletion(true);
    }

    @Test
    public void runTest() throws Exception {
        (new Test_JobParameter()).main(new String[]{});
    }
}
