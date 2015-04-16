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

import com.scaleoutsoftware.soss.client.*;
import com.scaleoutsoftware.soss.client.map.BulkLoader;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * Test that uses the NamedCache for a MapReduce Job's input and output with writable objects.
 */
public class Test_GridInputFormat_Writables {
    private static final String INPUT_CACHE = "Input cache";
    private static final String OUTPUT_CACHE = "Output cache";

    public static class WordCountMapper extends Mapper<CachedObjectId, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        /*
         * Optimized map function that doesn't use "throw away objects". Operates by analyzing an array of bytes and
         * finds words by cutting the array from the start of the word to the next space character(0x20 - in utf8).
         */
        public void map(CachedObjectId key, Text value, Context context) throws IOException, InterruptedException {
            int from = 0;
            int to = 0;
            int length = 0;
            final int SPACE = 0x20;
            byte buf[];
            Text word = new Text();
            buf = value.getBytes();
            length = value.getLength();
            from = 0;
            while (true) {
                for (to = from; to < length; to++) {
                    if (buf[to] == SPACE) break;
                }
                word.set(buf, from, to - from);
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

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, String> {

        public WordCountReducer() {           }
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            IntWritable value = null;
            int sum = 0;
            for (IntWritable val : values) {
                if (value == null) value = val;
                sum += val.get();
            }
            value.set(sum);
            context.write(key, "" + key + " was used " + value.get() + "times");

        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public IntSumReducer() {
        }

        IntWritable value;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            value = null;
            int sum = 0;
            for (IntWritable val : values) {
                if (value == null) value = val;
                sum += val.get();
            }
            value.set(sum);

            context.write(key, value);

        }
    }

    public static void main(String argv[]) throws Exception {
        InvocationGrid grid = HServerJob.getInvocationGridBuilder("MyGrid" + System.currentTimeMillis())
                .addJar("/path/to/your/classes.jar")
                .addClass(Test_GridInputFormat_Writables.class)
                .setLibraryPath("hadoop-1.2.1")
                .load();

        // Creates input and output named cache
        NamedCache inputCache = CacheFactory.getCache(INPUT_CACHE);
        NamedCache outputCache = CacheFactory.getCache(OUTPUT_CACHE);

        inputCache.setCustomSerialization(new WritableSerializer(Text.class));
        inputCache.setAllowClientCaching(false);


        inputCache.clear();
        outputCache.clear();

        // Sets up the Hadoop job, which uses GridInputFormat and GridOutputFormat
        Configuration conf = new Configuration();
        HServerJob job = new HServerJob(conf, "Sample job", false, grid);


        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(String.class);

        job.setInputFormatClass(NamedCacheInputFormat.class);
        job.setOutputFormatClass(GridOutputFormat.class);
        job.setNumReduceTasks(argv.length == 2 ? new Integer(argv[0]) : 20);

        // Setting the input format properties: input object class and input cache
        NamedCacheInputFormat.setNamedCache(job, inputCache, Text.class);

        // Setting the output cache
        GridOutputFormat.setNamedCache(job, OUTPUT_CACHE);

        // Puts 100k strings into the input cache, five of them contain "foo"
        int numstrings = argv.length == 2 ? new Integer(argv[1]) : 10000;
        for (int i = 0; i < numstrings; i++) {
            inputCache.put(UUID.randomUUID(), i % 2 == 0 ? new Text("The quick brown fox jumps over the lazy dog") : new Text("Few black taxis drive up major roads on quiet hazy nights"));
        }

        // Run the Hadoop job, to find the strings containing "foo" and put them in the output cache
        long time = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job done in " + (System.currentTimeMillis() - time));

        // Validation: we should have five strings in the output cache
        for (CachedObjectId id : outputCache.query(new NamedCacheFilter())) {
            System.out.println("String found: " + outputCache.get(id));
        }

        grid.unload();
    }

    @Test
    public void runTest() throws Exception {
        (new Test_GridInputFormat_Writables()).main(new String[]{});
    }
}
