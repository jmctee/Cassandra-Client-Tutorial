//Copyright 2012 Joe McTee
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.jeklsoft.cassandraclient.hector;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.jeklsoft.cassandraclient.BaseReadingsTest;
import com.jeklsoft.cassandraclient.TestUtils;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

public class HectorTest extends BaseReadingsTest {

    public static me.prettyprint.hector.api.Keyspace configureHectorAccessToCassandra(String cassandraHostname, Integer cassandraPort,
                                                                                      String cassandraClusterName, String cassandraKeySpaceName,
                                                                                      String configurationPath, List<String> cassandraCommands) throws Exception {

        try {
            if (StringUtils.isNotEmpty(configurationPath) && (cassandraCommands != null) && (!cassandraCommands.isEmpty())) {
                TestUtils.initializeEmbeddedCassandra(configurationPath, cassandraCommands, cassandraHostname, cassandraPort);
            }

            CassandraHostConfigurator configurator = new CassandraHostConfigurator(cassandraHostname + ":" + cassandraPort);
            Cluster cluster = HFactory.getOrCreateCluster(cassandraClusterName, configurator);
            me.prettyprint.hector.api.Keyspace keyspace = HFactory.createKeyspace(cassandraKeySpaceName, cluster);
            return keyspace;
        }
        catch (Exception e) {
            throw new RuntimeException("Error configuring access", e);
        }
    }

}
