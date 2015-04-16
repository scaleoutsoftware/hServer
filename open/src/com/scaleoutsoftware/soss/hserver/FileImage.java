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
import com.scaleoutsoftware.soss.hserver.interop.BucketStore;
import com.scaleoutsoftware.soss.hserver.interop.BucketStoreFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * This image subclass is used for the input formats which subclass the  {@link FileInputFormat}.
 * In that case image ID string consists of the concatenated list of the file paths and the
 * specific input format used for that job.
 */
class FileImage extends GridImage {
    private List<String> filesPaths;
    private Map<String, Long> modificationDate = new HashMap<String, Long>();
    private Map<String, Long> lengths = new HashMap<String, Long>();


    /**
     * If the file list and input format match but some of the files were appended this method will calculate additional
     * splits to be recorded. The already recorded part of the dataset is going to be served from StateServer store.
     */
    @Override
    public boolean checkCurrent(JobContext context, InputFormat format) throws StateServerException, IOException, InterruptedException {
        if (!(format instanceof FileInputFormat))
            throw new IllegalArgumentException("Unexpected InputFormat type " + format.getClass().getName());
        List<FileStatus> newFiles = getFiles(context, (FileInputFormat) format);
        if (newFiles.size() != filesPaths.size()) return false;
        Collections.sort(newFiles);
        for (FileStatus newFile : newFiles) {
            if (newFile.getModificationTime() != modificationDate.get(newFile.getPath().toString())) {
                if (context.getConfiguration().getBoolean(DatasetInputFormat.enableAppendsPropertyName, false)) {
                    addNewSplits(context, format);
                    BucketStore bucketStore = BucketStoreFactory.getBucketStore(imageIdString);
                    bucketStore.writeImage(this, false);
                } else {
                    return false;
                }
            }
        }
        return true;

    }

    /**
     * ID string is constructed based on the name of the file-based input format and filenames.
     */
    @Override
    public void initializeImageIDString(JobContext context, InputFormat format) {
        if (format instanceof FileInputFormat) {
            filesPaths = new ArrayList<String>();
            List<FileStatus> files = getFiles(context, (FileInputFormat) format);
            Collections.sort(files);
            StringBuilder buffer = new StringBuilder(this.getClass().getName());
            buffer.append(format.getClass().getName());
            for (FileStatus file : files) {
                String filePath = file.getPath().toString();
                filesPaths.add(filePath);
                modificationDate.put(filePath, file.getModificationTime());
                lengths.put(filePath, file.getLen());
                buffer.append(file.getPath());
            }
            imageIdString = buffer.toString();
        }
    }

    /**
     * Calculates additional splits in case files were modified since the image was last recorded. It relies
     * on the fact that HDFS only allows appending the files. The {@link InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}
     * is called to get the split list, and then already recorded splits are remowed from that list. In case of partially recorded split,
     * this split is truncated, so it contains only appended part.
     *
     * @param context job context
     * @param format  input format
     */
    @SuppressWarnings("unchecked")
    void addNewSplits(JobContext context, InputFormat format) throws IOException, InterruptedException {
        List<InputSplit> newSplits = format.getSplits(context);
        for (InputSplit inputSplit : newSplits) {
            FileSplit split = (FileSplit) inputSplit;
            String path = split.getPath().toString();
            if (!filesPaths.contains(path)) throw new IOException("No such file in the recorded image.");
            long currentFileLength = lengths.get(path);

            long begin = split.getStart();
            if (begin >= currentFileLength) //New split is entirely in the appended part of the file
            {
                splits.add(new ImageInputSplit(inputSplit, getImageIdString(), creationTimestamp, splits.size()));
                continue;
            }
            long end = begin + split.getLength();
            if (end <= currentFileLength) continue; //New split is entirely in the recorded area, we don't need it

            FileSplit additionalSplit = new FileSplit(split.getPath(), currentFileLength, end - currentFileLength, split.getLocations());
            splits.add(new ImageInputSplit(additionalSplit, getImageIdString(), creationTimestamp, splits.size()));
        }
        List<FileStatus> files = getFiles(context, (FileInputFormat) format);
        for (FileStatus file : files) {
            String filePath = file.getPath().toString();
            modificationDate.put(filePath, file.getModificationTime());
            lengths.put(filePath, file.getLen());

        }
    }


    /**
     * Gets the list of input file for that job.
     *
     * @param context job context
     * @param format  input format
     */
    @SuppressWarnings("unchecked")
    private List<FileStatus> getFiles(JobContext context, FileInputFormat format) {
        try {
            Class clazz = FileInputFormat.class;

            Class[] paramTypes = {JobContext.class};
            Method m = clazz.getDeclaredMethod("listStatus", paramTypes);

            Object[] arguments = {context};
            m.setAccessible(true);
            return (List<FileStatus>) m.invoke(format, arguments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
