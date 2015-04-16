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

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * This class is used to retrieve job parameter object.
 */
public class JobParameter {
    /**
     * This method can be called from the mapper or the reducer to retrieve the job parameter
     * object. This job parameter object is set at the job invocation time by calling {@link HServerJob#setJobParameter(Object)},
     * and is distributed to all worker nodes running mappers and reducers.
     * This method is thread safe, so it can be called by multiple instances of the mapper concurrently.
     *
     * @param configuration configuration (from context)
     * @return parameter object
     * @throws IOException if a ScaleOut hServer access error occurred
     */
    public static Object get(Configuration configuration) throws IOException {
        return InvocationParameters.retrieveFromCache(configuration.getInt(HServerParameters.INVOCATION_ID, 0)).getJobParameter();
    }
}
