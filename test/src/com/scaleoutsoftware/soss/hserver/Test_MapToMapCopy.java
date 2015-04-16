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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test that copies values from the input map to the output map (no logic in map/reduce classes).
 */
public class Test_MapToMapCopy {
    public static void main(String argv[]) throws Exception {
        final NamedMap<IntWritable, Text> inputMap = NamedMapFactory.getMap("map-i", new WritableSerializer(IntWritable.class), new WritableSerializer(Text.class));
        final NamedMap<IntWritable, Text> outputMap = NamedMapFactory.getMap("map-o", new WritableSerializer(IntWritable.class), new WritableSerializer(Text.class));
        inputMap.clear();
        outputMap.clear();
        Thread.sleep(10000);
        BulkLoader<IntWritable, Text> put = inputMap.getBulkLoader();
        String content = "xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        Text contentW = new Text(content);

        IntWritable count = new IntWritable();
        for (int i = 0; i < 1000; i++) {
            count.set(i);
            put.put(count, contentW);
        }
        put.close();



        InvocationGrid grid = HServerJob.getInvocationGridBuilder("MyGrid" + System.currentTimeMillis())
                .addClass(Test_MapToMapCopy.class)
                .load();

        HServerJob job;
        Configuration configuration;

        for (int i = 0; i < 100; i++) {
            // MMF
            configuration = new Configuration();
            configuration.setInt("mapred.hserver.setting.reducer.usememorymappedfiles", 1);
            configuration.setInt("mapred.hserver.setting.namedmap.usememorymappedfiles", 1);
            configuration.setInt("mapred.hserver.setting.map.maxtempmemorykb", 100000);
            job = new HServerJob(configuration, "Sample job");
            job.setGrid(grid);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(NamedMapInputFormat.class);
            job.setOutputFormatClass(GridOutputFormat.class);
            NamedMapInputFormat.setNamedMap(job, inputMap);
            NamedMapInputFormat.setSuggestedNumberOfSplits(job, 64);
            GridOutputFormat.setNamedMap(job, outputMap);
            job.waitForCompletion(false);
            assertEquals(inputMap.size(), outputMap.size());
            outputMap.clear();
        }
        grid.unload();
    }

    @Test
    public void runTest() throws Exception {
        for (int i = 0; i < 1; i++) {
            (new Test_MapToMapCopy()).main(new String[]{});
        }
    }
}
