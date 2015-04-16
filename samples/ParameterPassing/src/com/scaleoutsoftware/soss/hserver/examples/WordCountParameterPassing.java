/*
 Copyright (c) 2013 by ScaleOut Software, Inc.

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
import com.scaleoutsoftware.soss.client.map.BulkLoader;
import com.scaleoutsoftware.soss.client.map.NamedMap;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import com.scaleoutsoftware.soss.hserver.GridOutputFormat;
import com.scaleoutsoftware.soss.hserver.HServerJob;
import com.scaleoutsoftware.soss.hserver.JobParameter;
import com.scaleoutsoftware.soss.hserver.NamedMapInputFormat;
import com.scaleoutsoftware.soss.hserver.WritableSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 * This is a modified word count program which uses ScaleOut hServer
 * to perform the MapReduce job and runs the analysis on a {@link NamedMap} to
 * retrieve the count of the words between a minimum word length
 * and a maximum word length
 */
public class WordCountParameterPassing {
    private final static String SAMPLE_INPUT = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod " +
            "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, " +
            "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. " +
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore " +
            "eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt " +
            "in culpa qui officia deserunt mollit anim id est laborum.";

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private MapArguments mapArgs = null;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                if (temp.length() >= mapArgs.minWordLength &&
                        temp.length() <= mapArgs.maxWordLength) {
                    word.set(temp);
                    context.write(word, one);
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // pull the MapArguments from the configuration
            mapArgs = (MapArguments) JobParameter.get(context.getConfiguration());
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class MapArguments implements Serializable {
        private static final long serialVersionUID = 1L;
        private int minWordLength;
        private int maxWordLength;

        MapArguments(int min, int max) {
            minWordLength = min;
            maxWordLength = max;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new RuntimeException("Required args: wordMinLength wordMaxLength");
        }

        int minLength = Integer.parseInt(args[0]);
        int maxLength = Integer.parseInt(args[1]);

        // Create parameter argument to send to the reducers
        MapArguments mapArgs = new MapArguments(minLength, maxLength);

        // Create the invocation grid
        InvocationGrid grid = HServerJob.getInvocationGridBuilder("WordCountIG")
                .addClass(TokenizerMapper.class)
                .addClass(IntSumReducer.class)
				.addClass(MapArguments.class)
                .load();

        // Create a default configuration
        Configuration conf = new Configuration();

        // Create the input map
        NamedMap<IntWritable, Text> inputMap = NamedMapFactory.getMap("InputMap",
                new WritableSerializer<IntWritable>(IntWritable.class),
                new WritableSerializer<Text>(Text.class));

        // Create the output map
        NamedMap<Text, IntWritable> outputMap = NamedMapFactory.getMap("OutputMap",
                new WritableSerializer<Text>(Text.class),
                new WritableSerializer<IntWritable>(IntWritable.class));

        // Clear the input and output maps
        inputMap.clear();
        outputMap.clear();

        // Create a BulkPut object
        BulkLoader<IntWritable, Text> loader = inputMap.getBulkLoader();

        IntWritable key = new IntWritable();
        Text value = new Text();

        // Build the input map from generated text
        Scanner scanner = new Scanner(SAMPLE_INPUT);

        for (int count = 0; scanner.hasNext(); count++) {
            value.set(scanner.next());
            key.set(count);
            loader.put(key, value);
        }

        scanner.close();

        // Close the bulk loader
        loader.close();

        // Assign the invocation grid to the maps, so parallel operations can be performed
        inputMap.setInvocationGrid(grid);
        outputMap.setInvocationGrid(grid);

        // Create an hServer job
        HServerJob job = new HServerJob(conf, "word count", false, grid);
        job.setJarByClass(WordCountParameterPassing.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(NamedMapInputFormat.class);
        job.setOutputFormatClass(GridOutputFormat.class);

        // Pass the map arguments object to the job
        job.setJobParameter(mapArgs);

        // Set named maps for the input and output formats
        NamedMapInputFormat.setNamedMap(job, inputMap);
        GridOutputFormat.setNamedMap(job, outputMap);

        // Execute the job
        job.waitForCompletion(true);

        // Unload the invocation grid
        grid.unload();

        // Output resulting words and their frequencies
        Iterable<Text> results = outputMap.keySet();
        System.out.println("Following words were longer than " + mapArgs.minWordLength + " and shorter than " + mapArgs.maxWordLength + ":");
        for (Text word : results) {
            System.out.println("\"" + word.toString() + "\" was used " + outputMap.get(word) + " times.");
        }
    }
}
