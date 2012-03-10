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

package com.jeklsoft.cassandraclient.astyanax;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jeklsoft.cassandraclient.BaseReadingsTest;
import com.jeklsoft.cassandraclient.TestUtils;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class TestAstyanaxProtocolBufferWithStandardColumnExample extends BaseReadingsTest {
    private static final Logger log = Logger.getLogger(TestAstyanaxProtocolBufferWithStandardColumnExample.class);

    private static final Boolean useEmbeddedCassandra = true;

    private static AstyanaxContext<Keyspace> context;
    private static Keyspace keyspace;
    private static ColumnFamily<UUID, DateTime> CF_USER_INFO;

    private static int port = cassandraPort;

    @BeforeClass
    public static void configureCassandra() throws Exception {

        if (useEmbeddedCassandra) {
            port = cassandraEmbeddedPort;

            List<String> cassandraCommands = new ArrayList<String>();
            cassandraCommands.add("create keyspace " + cassandraKeySpaceName + ";");
            cassandraCommands.add("use " + cassandraKeySpaceName + ";");
            cassandraCommands.add("create column family " + columnFamilyName + ";");

            TestUtils.initializeEmbeddedCassandra(configurationPath, cassandraCommands,
                    cassandraHostname, port);
        }

        AstyanaxConfiguration configuration = new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE);

        ConnectionPoolConfiguration connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("AstyanaxPool")
                .setPort(port)
                .setMaxConnsPerHost(1)
                .setSeeds(cassandraHostname + ":" + port);

        ConnectionPoolMonitor connectionPoolMonitor = new CountingConnectionPoolMonitor();

        ThriftFamilyFactory factory = ThriftFamilyFactory.getInstance();

        context = new AstyanaxContext.Builder()
                .forCluster(cassandraClusterName)
                .forKeyspace(cassandraKeySpaceName)
                .withAstyanaxConfiguration(configuration)
                .withConnectionPoolConfiguration(connectionPoolConfiguration)
                .withConnectionPoolMonitor(connectionPoolMonitor)
                .buildKeyspace(factory);

        context.start();

        keyspace = context.getEntity();

        CF_USER_INFO = new ColumnFamily<UUID, DateTime>(columnFamilyName,
                UUIDSerializer.get(),   // Key Serializer
                DateTimeSerializer.get());  // Column Serializer
    }

    @Test
    public void testAstyanaxAccess() {

        AstyanaxProtocolBufferWithStandardColumnExample example =
                new AstyanaxProtocolBufferWithStandardColumnExample(keyspace, CF_USER_INFO);

        runAccessorTest(example);
    }
}
