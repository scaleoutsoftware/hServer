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
