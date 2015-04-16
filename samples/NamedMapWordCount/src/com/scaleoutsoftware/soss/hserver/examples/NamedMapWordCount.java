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

/*
* This sample is based on the WordCount.java sample program distributed as part
* of Apache Hadoop 1.2.0.
*/

package com.scaleoutsoftware.soss.hserver.examples;

import com.scaleoutsoftware.soss.client.InvocationGrid;
import com.scaleoutsoftware.soss.client.InvokeException;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import com.scaleoutsoftware.soss.client.map.QueryCondition;
import com.scaleoutsoftware.soss.hserver.GridOutputFormat;
import com.scaleoutsoftware.soss.hserver.HServerJob;
import com.scaleoutsoftware.soss.hserver.NamedMapInputFormat;
import com.scaleoutsoftware.soss.hserver.WritableSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * This is modified word count program, which uses ScaleOut hServer
 * to perform the MR job and runs the query on {@link NamedMap} to
 * retrieve the words that are used more often than specified threshold.
 */
public class NamedMapWordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: wordcount <input map> <output map> <threshold>");
            System.exit(2);
        }

        final int threshold = new Integer(otherArgs[2]);

        NamedMap<IntWritable, Text> inputMap = NamedMapFactory.getMap(otherArgs[0],
                new WritableSerializer<IntWritable>(IntWritable.class),
                new WritableSerializer<Text>(Text.class));

        NamedMap<Text, IntWritable> outputMap = NamedMapFactory.getMap(otherArgs[1],
                new WritableSerializer<Text>(Text.class),
                new WritableSerializer<IntWritable>(IntWritable.class));

        //Create the invocation grid
        InvocationGrid grid = HServerJob.getInvocationGridBuilder("WordCountIG").
                addJar("wordcount.jar").
                load();

        //Create hServer job
        Job job = new HServerJob(conf, "word count", false, grid);
        job.setJarByClass(NamedMapWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(NamedMapInputFormat.class);
        job.setOutputFormatClass(GridOutputFormat.class);

        //Set named maps as input and output
        NamedMapInputFormat.setNamedMap(job, inputMap);
        GridOutputFormat.setNamedMap(job, outputMap);

        //Execute job
        job.waitForCompletion(true);

        //Assign invocation grid to the map, so parallel operation can be performed
        outputMap.setInvocationGrid(grid);

        //Run query to find words that are used more than threshold frequency
        Iterable<Text> words = outputMap.executeParallelQuery(new UsageFrequencyCondition(threshold));

        //Unload the invocation grid
        grid.unload();

        //Output resulting words and their frequencies
        System.out.println("Following words were used more than " + threshold + " times:");
        for (Text word : words) {
            System.out.println("\"" + word.toString() + "\" was used " + outputMap.get(word) + " times.");
        }
    }

    //Implementation of the query condition. Condition is true if
    //the usage frequency exceeds threshold frequency
    static class UsageFrequencyCondition implements QueryCondition<Text, IntWritable> {
        private int frequency;

        UsageFrequencyCondition(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public boolean check(Text key, IntWritable value) throws InvokeException {
            return value.get() > frequency;
        }
    }
}
