package com.jeklsoft.cassandraclient;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddedCassandra {

    private static final String embeddedCassandraHostname = "localhost";
    private static final Integer embeddedCassandraPort = 9160;
    private static final String embeddedCassandraKeySpaceName = "TestKeyspaceName";
    private static final String columnFamilyName = "TestColumnName";
    private static final String configurationPath = "/tmp/cassandra";

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

        EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
        embeddedCassandra.setCleanCassandra(true);
        embeddedCassandra.setCassandraStartupCommands(cassandraCommands);
        embeddedCassandra.setHostname(embeddedCassandraHostname);
        embeddedCassandra.setHostport(embeddedCassandraPort);
        embeddedCassandra.setCassandraConfigDirPath(configurationPath);
        embeddedCassandra.init();
    }
}
