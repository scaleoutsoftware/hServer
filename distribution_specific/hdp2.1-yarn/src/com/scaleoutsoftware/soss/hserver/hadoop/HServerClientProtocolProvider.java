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
package com.scaleoutsoftware.soss.hserver.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HServerClientProtocolProvider extends ClientProtocolProvider {
    private final static String HSERVER_FRAMEWORK_NAME = "hserver-yarn";

    @Override
    public ClientProtocol create(Configuration configuration) throws IOException {
        if (HSERVER_FRAMEWORK_NAME.equals(configuration.get(MRConfig.FRAMEWORK_NAME))) {
           return new HServerClientProtocol(configuration);
        }
        return null;
    }

    @Override
    public ClientProtocol create(InetSocketAddress inetSocketAddress, Configuration configuration) throws IOException {
       return create(configuration);
    }

    @Override
    public void close(ClientProtocol clientProtocol) throws IOException {
        //nop
    }
}
