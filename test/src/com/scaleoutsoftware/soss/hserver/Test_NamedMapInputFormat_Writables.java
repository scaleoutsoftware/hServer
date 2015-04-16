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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;

/**
 * Test that uses the NamedMap for a MapReduce Job's input and output with writable objects.
 */
public class Test_NamedMapInputFormat_Writables {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
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
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            buf = value.getBytes();
            length = value.getLength();
            from = 0;
            while (true) {
                for (to = from; to < length; to++) {
                    if (buf[to] == SPACE) break;
                }
                word.set(buf, from, to - from);
                // context.write(word, one);
                context.write(word, one);
                from = to + 1;
                if (from >= length) return;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public WordCountReducer() {
        }

        LongWritable value = null;

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            value = null;
            int sum = 0;
            for (LongWritable val : values) {
                if (value == null) value = val;
                sum += val.get();
            }
            value.set(sum);
            context.write(key, value);

        }
    }

    public static class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        public WordCountCombiner() {
        }

        LongWritable value = null;

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            value = null;
            int sum = 0;
            for (LongWritable val : values) {
                if (value == null) value = val;
                sum += val.get();
            }
            value.set(sum);
            context.write(key, value);

        }
    }

    public static void main(String argv[]) throws Exception {
        InvocationGrid grid = HServerJob.getInvocationGridBuilder("MyGrid" + System.currentTimeMillis())
                .addJar("C:\\development\\hserver\\bundle.jar")
                .load();

        // Create input and output map
        NamedMap<LongWritable, Text> inputMap = NamedMapFactory.getMap("inputMap", new WritableSerializer<LongWritable>(LongWritable.class), new WritableSerializer<Text>(Text.class));
        NamedMap<Text, LongWritable> outputMap = NamedMapFactory.getMap("outputMap", new WritableSerializer<Text>(Text.class), new WritableSerializer<LongWritable>(LongWritable.class));
        inputMap.clear();
        outputMap.clear();

        Thread.sleep(10000);

        // Puts 100k strings into the input map
        int numstrings = argv.length == 1 ? new Integer(argv[0]) : 30000000;
        BulkLoader<LongWritable, Text> put = inputMap.getBulkLoader();
        Text a = new Text("The quick brown fox jumps over the lazy dog");
        Text b = new Text("Few black taxis drive up major roads on quiet hazy nights");
        LongWritable num = new LongWritable();
        for (int i = 0; i < numstrings; i++) {
            num.set(i);
            put.put(num, i % 2 == 0 ? a : b);
        }
        put.close();
        Thread.sleep(10000);

        // Sets up the Hadoop job, which uses GridInputFormat and GridOutputFormat
        Configuration conf = new Configuration();
        HServerJob job = new HServerJob(conf, "Sample job");
        job.setGrid(grid);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(NamedMapInputFormat.class);
        job.setOutputFormatClass(GridOutputFormat.class);

        // Setting the input format properties: input object class and input cache
        NamedMapInputFormat.setNamedMap(job, inputMap);

        // Setting the output cache
        GridOutputFormat.setNamedMap(job, outputMap);

        // Run the Hadoop job, to find the strings containing "foo" and put them in the output cache
        long time = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job done in " + (System.currentTimeMillis() - time));

        // Validation
        for (Text key : outputMap.keySet()) {
            System.out.println(key + " was used " + outputMap.get(key) + "times");
        }
        grid.unload();
    }

    @Test
    public void runTest() throws Exception {
        (new Test_NamedMapInputFormat_Writables()).main(new String[]{});
    }
}
