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
