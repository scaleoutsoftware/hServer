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

import com.scaleoutsoftware.soss.client.CustomSerializer;
import com.scaleoutsoftware.soss.client.ObjectNotSupportedException;
import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import com.scaleoutsoftware.soss.client.map.impl.DefaultSerializer;
import com.scaleoutsoftware.soss.hserver.hive.HServerHiveStorageHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;

/**
 * Input format used by SOSS Hive storage handler.
 */
public class HiveNamedMapInputFormatMapred extends HiveInputFormat<Text, Text> {
    public static final Log LOG = LogFactory.getLog(HiveNamedMapInputFormatMapred.class);

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        String mapName = jobConf.get(HServerHiveStorageHandler.MAP_NAME);
        if (mapName == null || mapName.length() == 0) {
            throw new IOException("Input format is not configured with a valid NamedMap.");
        }

        int mapId = NamedMapFactory.getMap(mapName, new StubSerializer(), new StubSerializer()).getMapId();
        List<org.apache.hadoop.mapreduce.InputSplit> splits;

        try {
            splits = GridInputFormat.getSplits(mapId, i);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        InputSplit[] wrappedSpilts = new InputSplit[splits.size()];

        Iterator splitIterator = splits.iterator();

        Path dummyPath = FileInputFormat.getInputPaths(jobConf)[0];
        LOG.debug("Using dummy path for splits:" + dummyPath);

        //Wrap splits to conform to mapred API
        for (i = 0; i < wrappedSpilts.length; i++) {
            wrappedSpilts[i] = new BucketSplitMapred((BucketSplit) splitIterator.next(), dummyPath);
        }

        return wrappedSpilts;
    }


    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf configuration, Reporter reporter) throws IOException {
        String mapName = configuration.get(HServerHiveStorageHandler.MAP_NAME);

        int mapId = NamedMapFactory.getMap(mapName, new StubSerializer(), new StubSerializer()).getMapId();
        return new JsonRecordReader(inputSplit, configuration, mapId);
    }

    private class JsonRecordReader implements RecordReader<Text, Text> {
        private final RecordReader wrappedReader;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Text key = new Text();
        private final Text value = new Text();
        private long counter = 0;

        private JsonRecordReader(InputSplit inputSplit, Configuration configuration, int mapId) throws IOException {
            String valueSerializer = configuration.get(HServerHiveStorageHandler.VALUE_SERIALIZER, null);
            String valueType = configuration.get(HServerHiveStorageHandler.VALUE_TYPE, null);

            CustomSerializer serializer;

            if (valueSerializer != null && valueSerializer.length() > 0) {
                try {
                    serializer = (CustomSerializer) Class.forName(valueSerializer).newInstance();
                    if (valueType != null && valueType.length() > 0) {
                        serializer.setObjectClass(Class.forName(valueType));
                    }
                } catch (Exception e) {
                    throw new IOException("Cannot instantiate value serializer/deserializer.", e);
                }
            } else {
                //Just use standard Java serialization
                serializer = new DefaultSerializer();
            }
            wrappedReader = new NamedMapInputFormatMapred.NamedMapRecordReaderMapred(inputSplit, configuration, mapId, new StubSerializer(), serializer);
        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            if (wrappedReader.next(null, null)) {
                //Serialize value using JSON serializer
                Object valueObject = wrappedReader.createValue();
                StringWriter valueWriter = new StringWriter();
                objectMapper.writeValue(valueWriter, valueObject);

                key.set("" + this.hashCode() + (counter++)); //Just make sure that keys are unique
                value.set(valueWriter.toString());

                return true;
            }

            return false;
        }

        @Override
        public Text createKey() {
            return key;
        }

        @Override
        public Text createValue() {
            return value;
        }

        @Override
        public long getPos() throws IOException {
            return wrappedReader.getPos();
        }

        @Override
        public void close() throws IOException {
            wrappedReader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return wrappedReader.getProgress();
        }
    }

    public static class StubSerializer extends CustomSerializer {
        final Object dummy = new Object();

        @Override
        public void serialize(OutputStream outputStream, Object o) throws ObjectNotSupportedException, IOException {
            //Do nothing
        }

        @Override
        public Object deserialize(InputStream stream) throws ObjectNotSupportedException, IOException, ClassNotFoundException {
            //Do nothing
            return dummy;
        }
    }

}
