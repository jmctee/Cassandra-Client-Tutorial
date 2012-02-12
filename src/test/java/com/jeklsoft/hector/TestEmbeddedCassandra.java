package com.jeklsoft.hector;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestEmbeddedCassandra {

    private static final String embeddedCassandraHostname = "localhost";
    private static final Integer embeddedCassandraPort = 9160;
    private static final String embeddedCassandraKeySpaceName = "TestKeyspaceName";
    private static final String columnFamilyName = "TestColumnName";
    private static final String configurationPath = "/tmp/cassandra";

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
