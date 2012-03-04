package com.jeklsoft.hector;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URL;
import java.util.List;

public class TestUtils {
    private static final Logger log = Logger.getLogger(TestUtils.class);

    public static me.prettyprint.hector.api.Keyspace configureHectorAccessToCassandra(String cassandraHostname, Integer cassandraPort,
                                                                                      String cassandraClusterName, String cassandraKeySpaceName,
                                                                                      String configurationPath, List<String> cassandraCommands) throws Exception {

        try {
            if (StringUtils.isNotEmpty(configurationPath) && (cassandraCommands != null) && (!cassandraCommands.isEmpty())) {
                initializeEmbeddedCassandra(configurationPath, cassandraCommands, cassandraHostname, cassandraPort);
            }

            CassandraHostConfigurator configurator = new CassandraHostConfigurator(cassandraHostname + ":" + cassandraPort);
            Cluster cluster = HFactory.getOrCreateCluster(cassandraClusterName, configurator);
            me.prettyprint.hector.api.Keyspace keyspace = HFactory.createKeyspace(cassandraKeySpaceName, cluster);
            return keyspace;
        } catch (Exception e) {
            log.log(Level.ERROR, "Error received", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    private static void initializeEmbeddedCassandra(String configurationPath, List<String> cassandraCommands,
                                                    String cassandraHostname, Integer cassandraPort) throws Exception {

        FileUtils.deleteDirectory(new File(configurationPath));

        URL stream = TestUtils.class.getClassLoader().getResource("cassandra.yaml");
        File cassandraYaml = new File(stream.toURI());

        FileUtils.copyFileToDirectory(cassandraYaml, new File(configurationPath));

        EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
        embeddedCassandra.setCleanCassandra(true);
        embeddedCassandra.setCassandraStartupCommands(cassandraCommands);
        embeddedCassandra.setHostname(cassandraHostname);
        embeddedCassandra.setHostport(cassandraPort);
        embeddedCassandra.setCassandraConfigDirPath(configurationPath);
        embeddedCassandra.init();
    }

}
