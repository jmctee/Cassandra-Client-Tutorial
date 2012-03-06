package com.jeklsoft.cassandraclient;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class TestUtils {

    public static EmbeddedCassandra initializeEmbeddedCassandra(String configurationPath, List<String> cassandraCommands,
                                                                String cassandraHostname, Integer cassandraPort) throws Exception {

        FileUtils.deleteDirectory(new File(configurationPath));

        URL stream = TestUtils.class.getClassLoader().getResource("cassandra.yaml");
        File cassandraYaml = new File(stream.toURI());

        FileUtils.copyFileToDirectory(cassandraYaml, new File(configurationPath));

        EmbeddedCassandra embeddedCassandra = EmbeddedCassandra.builder()
                .withCleanDataStore()
                .withStartupCommands(cassandraCommands)
                .withHostname(cassandraHostname)
                .withHostport(cassandraPort)
                .withCassandaConfigurationDirectoryPath(configurationPath)
                .build();

        return embeddedCassandra;
    }

}
