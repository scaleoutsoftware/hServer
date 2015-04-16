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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test that copies values from the input map to the output map (no logic in map/reduce classes).
 */
public class Test_MapToMapCopyMapred extends Configured implements Tool {

    public int run(String[] args) throws Exception{
        final NamedMap<IntWritable, Text> inputMap = NamedMapFactory.getMap("mapr-i", new WritableSerializer(IntWritable.class), new WritableSerializer(Text.class));
        final NamedMap<IntWritable, Text> outputMap = NamedMapFactory.getMap("mapr-o", new WritableSerializer(IntWritable.class), new WritableSerializer(Text.class));
        inputMap.clear();
        outputMap.clear();
        Thread.sleep(15000);
        BulkLoader<IntWritable, Text> put = inputMap.getBulkLoader();
        String content = "xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        Text contentW = new Text(content);
        IntWritable count = new IntWritable();
        int expectedSize = 10000;

        for (int i = 0; i < expectedSize; i++) {
            count.set(i);
            put.put(count, contentW);
        }
        put.close();
        InvocationGrid grid = HServerJob.getInvocationGridBuilder("MyGrid" + System.currentTimeMillis())
                .addClass(Test_MapToMapCopyMapred.class)
                .load();


        JobConf configuration = new JobConf(getConf(), Test_MapToMapCopyMapred.class);
        configuration.setInt("mapred.hserver.setting.reducer.usememorymappedfiles",0);
        configuration.setMapOutputKeyClass(IntWritable.class);
        configuration.setMapOutputValueClass(Text.class);
        configuration.setOutputKeyClass(IntWritable.class);
        configuration.setOutputValueClass(Text.class);
        configuration.setInputFormat(NamedMapInputFormatMapred.class);
        configuration.setOutputFormat(NamedMapOutputFormatMapred.class);
        NamedMapInputFormatMapred.setNamedMap(configuration, inputMap);
        NamedMapOutputFormatMapred.setNamedMap(configuration, outputMap);
        assertEquals(inputMap.size(), outputMap.size()+expectedSize); // should be 0 + expected
        HServerJobClient.runJob(configuration, false, grid);
        assertEquals(inputMap.size(), outputMap.size());
        inputMap.clear();
        outputMap.clear();
        grid.unload();
        return 1;
    }

    @Test
    public void runTest() throws Exception {
        int ret = ToolRunner.run(new Configuration(), new Test_MapToMapCopyMapred(), new String[]{});
    }
}
