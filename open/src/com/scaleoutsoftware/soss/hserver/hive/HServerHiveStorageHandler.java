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
package com.scaleoutsoftware.soss.hserver.hive;

import com.scaleoutsoftware.soss.client.map.NamedMapFactory;
import com.scaleoutsoftware.soss.hserver.HiveNamedMapInputFormatMapred;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.Map;

/**
 * SOSS storage handler, which provides Hive table view of the NamedMap,
 * enabling HQL queries on the NamedMap.
 */
public class HServerHiveStorageHandler implements HiveStorageHandler {
    public static final String MAP_NAME = "hserver.map.name";
    public static final String VALUE_SERIALIZER = "hserver.value.serializer";
    public static final String VALUE_TYPE = "hserver.value.type";

    private Configuration configuration;

    public HServerHiveStorageHandler() {
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HiveNamedMapInputFormatMapred.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return TextOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return JsonSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new HiveMetaHook() {
            @Override
            public void preCreateTable(Table table) throws MetaException {
            }

            @Override
            public void rollbackCreateTable(Table table) throws MetaException {
            }

            @Override
            public void commitCreateTable(Table table) throws MetaException {
            }

            @Override
            public void preDropTable(Table table) throws MetaException {
            }

            @Override
            public void rollbackDropTable(Table table) throws MetaException {
            }

            @Override
            public void commitDropTable(Table table, boolean deleteData) throws MetaException {
                //Do nothing, this storage handler is read onlyh
            }
        };
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        String mapName = tableDesc.getProperties().getProperty(MAP_NAME);
        String valueSerializer = tableDesc.getProperties().getProperty(VALUE_SERIALIZER);
        String valueType = tableDesc.getProperties().getProperty(VALUE_TYPE);

        jobProperties.put(MAP_NAME, mapName);

        if (valueSerializer != null) {
            jobProperties.put(VALUE_SERIALIZER, valueSerializer);
            if (valueType != null) {
                jobProperties.put(VALUE_TYPE, valueType);
            }
        }
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        String mapName = tableDesc.getProperties().getProperty(MAP_NAME);
        String valueSerializer = tableDesc.getProperties().getProperty(VALUE_SERIALIZER);
        String valueType = tableDesc.getProperties().getProperty(VALUE_TYPE);

        jobConf.set(MAP_NAME, mapName);

        if (valueSerializer != null) {
            jobConf.set(VALUE_SERIALIZER, valueSerializer);
            if (valueType != null) {
                jobConf.set(VALUE_TYPE, valueType);
            }
        }
    }

}
