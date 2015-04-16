/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scaleoutsoftware.soss.hserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import com.scaleoutsoftware.soss.client.InvocationGrid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

    /**
     * This is an example Hadoop Map/Reduce application.
     * It reads the text input files, breaks each line into words
     * and counts them. The output is a locally sorted list of words and the
     * count of how often they occurred.
     *
     * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
     *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
     */
    public class Test_WordCountMapred extends Configured implements Tool {

        /**
         * Counts the words in each line.
         * For each line of input, break the line into words and emit them as
         * (<b>word</b>, <b>1</b>).
         */
        public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(LongWritable key, Text value,
                            OutputCollector<Text, IntWritable> output,
                            Reporter reporter) throws IOException {
                String line = value.toString();
                StringTokenizer itr = new StringTokenizer(line);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    output.collect(word, one);
                }
            }
        }

        /**
         * A reducer class that just emits the sum of the input values.
         */
        public static class Reduce extends MapReduceBase
                implements Reducer<Text, IntWritable, Text, IntWritable> {

            public void reduce(Text key, Iterator<IntWritable> values,
                               OutputCollector<Text, IntWritable> output,
                               Reporter reporter) throws IOException {
                int sum = 0;
                while (values.hasNext()) {
                    sum += values.next().get();
                }
                output.collect(key, new IntWritable(sum));
            }
        }



        /**
         * The main driver for word count map/reduce program.
         * Invoke this method to submit the map/reduce job.
         * @throws IOException When there is communication problems with the
         *                     job tracker.
         */
        public int run(String[] args) throws Exception {
            JobConf conf = new JobConf(getConf(), Test_WordCountMapred.class);
            conf.setJobName("wordcount");

            // the keys are words (strings)
            conf.setOutputKeyClass(Text.class);
            // the values are counts (ints)
            conf.setOutputValueClass(IntWritable.class);

            conf.setMapperClass(MapClass.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);
            conf.setNumReduceTasks(0);

            String in = args.length == 2 ? args[0] : "random.txt";
            String out = args.length == 2 ? args[1] : "c:\\development\\mapred_output\\dir" + System.currentTimeMillis();

            FileInputFormat.setInputPaths(conf, new Path(in));
            FileOutputFormat.setOutputPath(conf, new Path(out));

            InvocationGrid grid = HServerJob.getInvocationGridBuilder("MyGrid" + System.currentTimeMillis()).
                    addJar("/path/to/your/jar").
                    load();

            // HERE IS STANDARD HADOOP INVOCATION
            //JobClient.runJob(conf);

            // HSERVER INVOCATION
            HServerJobClient.runJob(conf, false, grid);
            return 0;
        }

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new Test_WordCountMapred(), args);
            System.exit(res);
        }

    }
