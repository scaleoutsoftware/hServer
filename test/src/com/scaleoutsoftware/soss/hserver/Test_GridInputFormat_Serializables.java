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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Test that uses the NamedCache for a MapReduce Job's input and output with serializable objects.
 */
public class Test_GridInputFormat_Serializables {
    private static final String INPUT_CACHE = "Input cache";
    private static final String OUTPUT_CACHE = "Output cache";

    public static class WordCountMapper extends Mapper<CachedObjectId, Text, String, Integer> {
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
        public void map(CachedObjectId key, Text value, Context context) throws IOException, InterruptedException {

            buf = value.getBytes();
            length = value.getLength();
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

    public static class WordCountReducer extends Reducer<String, Integer, String, String> {
        public WordCountReducer() {
        }

        @Override
        public void reduce(String key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Integer val : values) {
                sum += val;
            }
            context.write(key, "" + key + " was used " + sum + "times");
        }
    }

    public static class Combiner extends Reducer<String, Integer, String, Integer> {
        public Combiner() {
        }

        @Override
        public void reduce(String key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
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
                .addClass(Test_GridInputFormat_Serializables.class)
                .setLibraryPath("hadoop-1.2.1")
                .load();


        // Creates input and output named cache
        NamedCache inputCache = CacheFactory.getCache(INPUT_CACHE);
        NamedCache outputCache = CacheFactory.getCache(OUTPUT_CACHE);

        inputCache.setCustomSerialization(new WritableSerializer(Text.class));
        inputCache.setAllowClientCaching(false);

        outputCache.setCustomSerialization(new WritableSerializer(Text.class));
        outputCache.setAllowClientCaching(false);

        inputCache.clear();
        outputCache.clear();

        // Sets up the Hadoop job, which uses GridInputFormat and GridOutputFormat
        Configuration conf = new Configuration();
        HServerJob job = new HServerJob(conf, "Sample job");

        job.setGrid(grid);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(Combiner.class);

        job.setMapOutputKeyClass(String.class);
        job.setMapOutputValueClass(Integer.class);
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(String.class);

        job.setInputFormatClass(NamedCacheInputFormat.class);
        job.setOutputFormatClass(GridOutputFormat.class);

        // Setting the input format properties: input object class and input cache
        NamedCacheInputFormat.setNamedCache(job, inputCache, Text.class);

        // Setting the output cache
        GridOutputFormat.setNamedCache(job, OUTPUT_CACHE);

        // Puts 100k strings into the input cache, five of them contain "foo"
        int numstrings = argv.length == 1 ? new Integer(argv[0]) : 100000;
        for (int i = 0; i < numstrings; i++) {
            inputCache.put(UUID.randomUUID(), i % 2 == 0 ? new Text("The quick brown fox jumps over the lazy dog") : new Text("Few black taxis drive up major roads on quiet hazy nights"));
        }

        // Run the MapReduce
        long time = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job done in " + (System.currentTimeMillis() - time));
        for (CachedObjectId id : outputCache.query(new NamedCacheFilter())) {
            System.out.println("String found: " + outputCache.get(id));
        }
        grid.unload();
    }

    @Test
    public void runTest() throws Exception {
        (new Test_GridInputFormat_Serializables()).main(new String[]{});
    }

}
