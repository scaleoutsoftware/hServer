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

import com.scaleoutsoftware.soss.client.CacheFactory;
import com.scaleoutsoftware.soss.client.CachedObjectId;
import com.scaleoutsoftware.soss.client.InvocationGrid;
import com.scaleoutsoftware.soss.client.NamedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

public class Test_SingleResultOptimisation_Writables {
    private static final String INPUT_CACHE = "Input cache";
    private static final String OUTPUT_CACHE = "Output cache";

    // Count the number of objects
    public static class MyMapper extends Mapper<CachedObjectId, Object, NullWritable, IntWritable> {

        private IntWritable one = new IntWritable(1);

        @Override
        public void map(CachedObjectId key, Object value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), one);
        }
    }

    public static class MyReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        IntWritable value;

        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
                .load();


        //Creates input and output named cache
        NamedCache inputCache = CacheFactory.getCache(INPUT_CACHE);
        NamedCache outputCache = CacheFactory.getCache(OUTPUT_CACHE);

        inputCache.setCustomSerialization(new WritableSerializer(Text.class));
        inputCache.setAllowClientCaching(false);

        outputCache.setCustomSerialization(new WritableSerializer(Text.class));
        outputCache.setAllowClientCaching(false);

        inputCache.clear();
        outputCache.clear();

        // Sets up the hServer job, which uses NamedCacheInputFormat and NamedCacheOutputFormat
        Configuration conf = new Configuration();
        HServerJob job = new HServerJob(conf, "Sample job", false, grid);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(NamedCacheInputFormat.class);

        // Setting the input format properties: input object class and input cache
        NamedCacheInputFormat.setNamedCache(job, inputCache, Object.class);

        NamedCacheInputFormat.setSuggestedNumberOfSplits(job, 1024);


        // Populate the NamedCache with random values...
        for (int i = 0; i < 1000; i++) {
            inputCache.put(UUID.randomUUID(), new String("foo"));
        }

        // Run the Hadoop job, to find the strings containing "foo" and put them in the output cache
        long time = System.currentTimeMillis();
        System.out.println("Result returned:" + job.runAndGetResult());
        System.out.println("Job done in " + (System.currentTimeMillis() - time));
        grid.unload();
    }

    @Test
    public void runTest() throws Exception {
        for(int i=0; i<100; i++){
            (new Test_SingleResultOptimisation_Writables()).main(new String[]{});
        }
    }

}
