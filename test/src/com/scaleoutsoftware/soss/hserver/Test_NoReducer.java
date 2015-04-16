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

import com.scaleoutsoftware.soss.client.da.DataAccessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

// Simple test to run a MapReduce job with no reducer
public class Test_NoReducer {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {


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
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            buf = value.getBytes();
            length = buf.length;
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

    // Function to create a local file and write random words to it
    public static void writeFile() throws Exception {
        // shortened alphabet for decreased number of randomly generated unique words
        String alphabet = "abcdefghijklmnopq";
        File file = new File("random.txt");

        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        Random r = new Random(System.currentTimeMillis());


        long size = 0;
        int alphabetSize = alphabet.length();
        while (size < 250 * 1000000) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                builder.append(alphabet.charAt(r.nextInt(alphabetSize)));
                builder.append(alphabet.charAt(r.nextInt(alphabetSize)));
                builder.append(alphabet.charAt(r.nextInt(alphabetSize)));
                builder.append(alphabet.charAt(r.nextInt(alphabetSize)));
                if (i > 0 && i % 10 == 0) {
                    builder.append("\n");
                } else builder.append(" ");
            }
            size += builder.length();
            bw.write(builder.toString());
        }
        bw.close();
    }


    public static void main(String[] args) throws Exception {

        writeFile();
        DataAccessor.clearAllObjects();


        Configuration conf = new Configuration();
        conf.setInt("mapred.hserver.setting.reducer.usememorymappedfiles",0);

        String in = args.length == 2 ? args[0] : "random.txt";
        String out = args.length == 2 ? args[1] : "c:\\development\\mapred_output\\dir" + System.currentTimeMillis();


        HServerJob job;

        job = new HServerJob(conf);
        job.setJarPath("/path/to/your/classes.jar");
        long time = System.currentTimeMillis();
        job.setJarByClass(Test_WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out + System.currentTimeMillis()));
        job.waitForCompletion(true);
        System.out.println("Job done in: " + (System.currentTimeMillis() - time));
    }


    @Test
    public void runTest() throws Exception {
        (new Test_NoReducer()).main(new String[]{});
    }

}
