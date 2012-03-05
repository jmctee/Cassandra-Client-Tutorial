package com.jeklsoft.cassandraclient;

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
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestAstyanaxProtocolBufferWithStandardColumnExample extends BaseReadingsTest {
    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperColumnExample.class);

    private static final Boolean useEmbeddedCassandra = true;

    private static AstyanaxContext<Keyspace> context;
    private static Keyspace keyspace;
    private static ColumnFamily<UUID, Long> CF_USER_INFO;

    @BeforeClass
    public static void configureCassandra() throws Exception {

        if (useEmbeddedCassandra) {
            List<String> cassandraCommands = new ArrayList<String>();
            cassandraCommands.add("create keyspace " + cassandraKeySpaceName + ";");
            cassandraCommands.add("use " + cassandraKeySpaceName + ";");
            cassandraCommands.add("create column family " + columnFamilyName + ";");

            TestUtils.initializeEmbeddedCassandra(configurationPath, cassandraCommands,
                    cassandraHostname, cassandraPort);
        }

        AstyanaxConfiguration configuration = new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE);

        ConnectionPoolConfiguration connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("AstyanaxPool")
                .setPort(cassandraPort)
                .setMaxConnsPerHost(1)
                .setSeeds(cassandraHostname + ":" + cassandraPort);

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

        CF_USER_INFO = new ColumnFamily<UUID, Long>(columnFamilyName,
                UUIDSerializer.get(),   // Key Serializer
                LongSerializer.get());  // Column Serializer
    }

    @Test
    public void testAstyanaxAccess() {

        AstyanaxProtocolBufferWithStandardColumnExample example =
                new AstyanaxProtocolBufferWithStandardColumnExample(keyspace, CF_USER_INFO);

        runAccessorTest(example);
    }
}
