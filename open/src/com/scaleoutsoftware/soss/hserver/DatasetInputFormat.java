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


import com.scaleoutsoftware.soss.client.da.StateServerException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * This class implements the input format that supports caching and replaying the input data set.
 * This input format acts as a wrapper around the underlying input format, which should be set by
 * {@link #setUnderlyingInputFormat(org.apache.hadoop.mapreduce.Job, Class)}.
 * On the first run, the produced record reader acts as a wrapper over the underlying record reader,
 * which intercepts and caches key-value pairs.
 * On subsequent runs, key-value pairs are replayed directly from the cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DatasetInputFormat<K extends Writable, V extends Writable> extends InputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(DatasetInputFormat.class);

    private static final String underlyingInputFormatPropertyName = "mapred.hserver.underlyingInputFormat";

    private static final String keySizePropertyName = "mapred.hserver.keySize";

    private static final String valueSizePropertyName = "mapred.hserver.valueSize";

    static final String enableAppendsPropertyName = "mapred.hserver.enableAppends";

    private InputFormat underlyingInputFormat = null;

    /**
     * Constructs an instance of the underlying input format to be wrapped by the dataset input format.
     *
     * @param configuration configuration
     * @return underlying input format
     */
    @SuppressWarnings("unchecked")
    private InputFormat<K, V> getUnderlyingInputFormat(Configuration configuration) throws IOException {
        if (underlyingInputFormat == null) {
            Class<? extends InputFormat> underlyingInputFormatClass = configuration.getClass(underlyingInputFormatPropertyName, null, InputFormat.class);
            if (underlyingInputFormatClass == null)
                throw new IOException("The underlying input format is not specified.");
            try {
                underlyingInputFormat = underlyingInputFormatClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return underlyingInputFormat;
    }

    /**
     * Gets the logical split of the input dataset. Splits are calculated either by the underlying input format and wrapped with {@link InputSplit} or
     * retrieved from the image stored in the StateServer.
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

        InputFormat<K, V> underlyingInputFormat = getUnderlyingInputFormat(context.getConfiguration());

        try {
            GridImage image = getImage(underlyingInputFormat.getClass());
            image = image.readOrCreateImage(context, underlyingInputFormat);
            return image.getSplits();
        } catch (StateServerException e) {
            LOG.error("Cannot access ScaleOut StateServer. Falling back to original split.", e);
            return underlyingInputFormat.getSplits(context);
        } catch (ClassNotFoundException e) {
            LOG.error("Image class was not found. Falling back to original split.", e);
            return underlyingInputFormat.getSplits(context);
        }


    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if (split instanceof ImageInputSplit) {
            InputFormat<K, V> underlyingInputFormat = getUnderlyingInputFormat(context.getConfiguration());
            RecordReader<K, V> underlyingRecordReader = underlyingInputFormat.createRecordReader(((ImageInputSplit) split).getFallbackInputSplit(), context);
            return new DatasetRecordReader<K, V>(underlyingRecordReader);
        } else {
            LOG.error("Input split is of unknown type, falling back to underlying input format.");
            InputFormat<K, V> underlyingInputFormat = getUnderlyingInputFormat(context.getConfiguration());
            return underlyingInputFormat.createRecordReader(split, context);
        }

    }

    /**
     * Sets the underlying input format that will be used as a source of
     * key-value pairs for caching.
     *
     * @param job   job to modify
     * @param clazz underlying input format class
     */
    public static void setUnderlyingInputFormat(Job job, Class<? extends InputFormat> clazz) {
        job.getConfiguration().setClass(underlyingInputFormatPropertyName, clazz, InputFormat.class);
    }

    /**
     * <p>
     * Optional property that can be used to set the size of the text key and value, if this size is known in advance.
     * </p>
     * If the key and value produced by the underlying input format is a Text object, and there is a known fixed 
     * size for keys and values, both server memory consumption and deserialization overhead can be optimized.
     * This is achieved by storing Text contiguously in a buffer and avoiding storing the length of each individual
     * object.
     *
     * @param job       job to modify
     * @param keySize   key size in bytes
     * @param valueSize value size in bytes
     */
    public static void setTextKeyValueSize(Job job, int keySize, int valueSize) {
        if (keySize <= 0 || valueSize <= 0) throw new IllegalArgumentException("Size should be positive number.");
        job.getConfiguration().setInt(keySizePropertyName, keySize);
        job.getConfiguration().setInt(valueSizePropertyName, valueSize);
    }

    /**
     * <p>
     * If this optional property is set to true, and the current file modification time does not match the
	 * modification time recorded in the cached image, it is assumed that the file was appended. 
	 * The input format will use the splits that were already recorded and add additional splits corresponding to
	 * the appended part which will be read from HDFS and recorded on the next run.
     * </p>
     * <p>
     * This option should be turned off when deleting the file and creating a new file in its place, because
     * it will make the <code>DatasetInputFormat</code> serve the recorded splits from the old file.
     * </p>
     * Appending is disabled by default--a new cached image will be created from scratch if the modification time does not match.
     *
     * @param job           job to modify
     * @param enableAppends <code>true</code> if appends are enabled.
     */
    @SuppressWarnings("unused")
    public static void setEnableAppends(Job job, boolean enableAppends) {
        job.getConfiguration().setBoolean(enableAppendsPropertyName, enableAppends);
    }

    /**
     * Creates a KVP-object with a fixed size key and value.
     *
     * @param configuration job configuration
     * @return KVP-object
     */
    static KeyValuePair createFixedKeyValueSizePair(Configuration configuration) {
        int keySize = configuration.getInt(keySizePropertyName, -1);
        int valueSize = configuration.getInt(valueSizePropertyName, -1);
        if (keySize > 0 && valueSize > 0) {
            return new KeyValuePair<Text, Text>(new Text(), keySize, new Text(), valueSize);
        }
        return null;
    }

    /**
     * Gets the image corresponding to the underlying input format.
     *
     * @param underlyingInputFormat underlying input format to create an image for
     * @return image instance
     */
    private static GridImage getImage(Class underlyingInputFormat) {
        if (FileInputFormat.class.isAssignableFrom(underlyingInputFormat)) {
            return new FileImage();
        } else {
            throw new IllegalArgumentException("Underlying input format is not supported");
        }
    }
}
