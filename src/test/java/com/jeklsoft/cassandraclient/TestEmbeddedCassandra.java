package com.jeklsoft.cassandraclient;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEmbeddedCassandra {

    private static final String embeddedCassandraHostname = "localhost";
    private static final Integer embeddedCassandraPort = 9160;
    private static final String embeddedCassandraKeySpaceName = "TestKeyspaceName";
    private static final String columnFamilyName = "TestColumnName";
    private static final String configurationPath = "target/cassandra";

    @BeforeClass
    public static void configureCassandra() throws Exception {

        FileUtils.deleteDirectory(new File(configurationPath));

        URL cassandraYamlUrl = TestEmbeddedCassandra.class.getClassLoader().getResource("cassandra.yaml");
        File cassandraYamlFile = new File(cassandraYamlUrl.toURI());

        FileUtils.copyFileToDirectory(cassandraYamlFile, new File(configurationPath));
    }

    @Test
    public void sunnyDayTest() throws Exception {
        List<String> cassandraCommands = new ArrayList<String>();
        cassandraCommands.add("create keyspace " + embeddedCassandraKeySpaceName + ";");
        cassandraCommands.add("use " + embeddedCassandraKeySpaceName + ";");
        cassandraCommands.add("create column family " + columnFamilyName + " with column_type = 'Super';");

        EmbeddedCassandra embeddedCassandra = EmbeddedCassandra.builder()
                .withCleanDataStore()
                .withStartupCommands(cassandraCommands)
                .withHostname(embeddedCassandraHostname)
                .withHostport(embeddedCassandraPort)
                .withCassandaConfigurationDirectoryPath(configurationPath)
                .build();

        assertNotNull(embeddedCassandra);
    }
}
