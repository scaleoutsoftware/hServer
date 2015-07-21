/*
 Copyright (c) 2015 by ScaleOut Software, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0
                                                                              j
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.scaleoutsoftware.soss.hserver;


import com.scaleoutsoftware.soss.client.util.SerializationMode;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the names of the HServer performance parameters, which can be modified
 * through configuration object and their default values.
 */
public class HServerParameters {
    public final static String MAP_OUTPUTCHUNKSIZE_KB = "mapred.hserver.setting.map.outputchunksizekb";
    public final static String MAP_HASHTABLESIZE = "mapred.hserver.setting.map.hashtablesize";
    public final static String MAP_MAXTEMPMEMORY_KB = "mapred.hserver.setting.map.maxtempmemorykb";
    public final static String MAP_THREADSPERHOST = "mapred.hserver.setting.map.threadsperhost";
    public final static String MAP_SPLITS_PER_CORE = "mapred.hserver.setting.map.splitspercore";
    public final static String MAP_LOW_COMBINING_MEMORY = "mapred.hserver.setting.map.lowmemory";

    public final static String REDUCE_INPUTCHUNKSIZE_KB = "mapred.hserver.setting.reducer.inputchunksizekb";
    public final static String REDUCE_CHUNKSTOREADAHEAD = "mapred.hserver.setting.reducer.chunkstoreadahead";
    public final static String REDUCE_CHUNKREADTIMEOUT = "mapred.hserver.setting.reducer.chunkreadtimeoutms";
    public final static String REDUCE_USEMEMORYMAPPEDFILES = "mapred.hserver.setting.reducer.usememorymappedfiles";

    public final static String CACHE_CHUNKSIZE_KB = "mapred.hserver.setting.cache.chunksizekb";
    public final static String CACHE_MAXTEMPMEMORY_KB = "mapred.hserver.setting.cache.maxtempmemorykb";

    public final static String CM_CHUNK_SIZE_KB = "mapred.hserver.setting.namedmap.chunksizekb";
    public final static String CM_CHUNKSTOREADAHEAD = "mapred.hserver.setting.namedmap.chunkstoreadahead";
    public final static String CM_USEMEMORYMAPPEDFILES = "mapred.hserver.setting.namedmap.usememorymappedfiles";

    public final static String IS_HSERVER_JOB = "mapred.hserver.hserverpresent";
    public final static String SORT_KEYS = "mapred.hserver.sortkeys";
    public final static String MAX_SLOTS = "mapred.hserver.maxslots"; //Maximum number of slots, 0 for no limit
    public final static String INVOCATION_ID = "mapred.hserver.invocationid";

    public final static String SERIALIZATION_MODE = "mapred.hserver.setting.serialization.mode";

    private final static Map<String, Object> _defaults = new HashMap<String, Object>();

    static {
        _defaults.put(MAP_OUTPUTCHUNKSIZE_KB, 128);
        _defaults.put(MAP_HASHTABLESIZE, 60000);
        _defaults.put(MAP_MAXTEMPMEMORY_KB, 500000);
        _defaults.put(MAP_THREADSPERHOST, 32);
        _defaults.put(MAP_LOW_COMBINING_MEMORY, 60);
        _defaults.put(MAP_SPLITS_PER_CORE, 1);
        _defaults.put(REDUCE_INPUTCHUNKSIZE_KB, 1024);
        _defaults.put(REDUCE_CHUNKSTOREADAHEAD, 100);
        _defaults.put(REDUCE_CHUNKREADTIMEOUT, 100000);
        _defaults.put(REDUCE_USEMEMORYMAPPEDFILES, 0);
        _defaults.put(CM_USEMEMORYMAPPEDFILES, 0);
        _defaults.put(CM_CHUNK_SIZE_KB, 128);
        _defaults.put(CM_CHUNKSTOREADAHEAD, 1000);
        _defaults.put(CACHE_CHUNKSIZE_KB, 256);
        _defaults.put(CACHE_MAXTEMPMEMORY_KB, 500000);
        _defaults.put(IS_HSERVER_JOB, false);
        _defaults.put(SORT_KEYS, true);
        _defaults.put(MAX_SLOTS, 0);
        _defaults.put(SERIALIZATION_MODE, SerializationMode.DEFAULT.ordinal());
    }

    /**
     * Gets the value for the given parameter.
     *
     * @param name          name of the parameter
     * @param configuration configuration object
     * @return value of the parameter
     */
    public static int getSetting(String name, Configuration configuration) {
        if (!_defaults.containsKey(name)) {
            throw new RuntimeException("Cannot find default value for property" + name);
        }
        if(configuration == null) return (Integer)_defaults.get(name);
        return configuration.getInt(name, (Integer) _defaults.get(name));
    }

    /**
     * Gets the value for the given parameter.
     *
     * @param name          name of the parameter
     * @param configuration configuration object
     * @return value of the parameter
     */
    public static boolean getBooleanSetting(String name, Configuration configuration) {
        if (!_defaults.containsKey(name)) {
            throw new RuntimeException("Cannot find default value for property" + name);
        }
        return configuration.getBoolean(name, false);
    }


    public static boolean isHServerJob(Configuration configuration) {
        return configuration.getBoolean(IS_HSERVER_JOB, false);
    }
}
