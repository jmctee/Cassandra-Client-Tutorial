package com.jeklsoft.cassandraclient.hector;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import me.prettyprint.hector.api.Keyspace;

public class TestHectorHeterogeneousSuperColumnExample extends HectorTest {

    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperColumnExample.class);

    private static final Boolean useEmbeddedCassandra = true;
    protected static Keyspace keyspace;

    @BeforeClass
    public static void configureCassandra() throws Exception {
        List<String> cassandraCommands = null;

        if (useEmbeddedCassandra) {
            cassandraCommands = new ArrayList<String>();
            cassandraCommands.add("create keyspace " + cassandraKeySpaceName + ";");
            cassandraCommands.add("use " + cassandraKeySpaceName + ";");
            cassandraCommands.add("create column family " + columnFamilyName + " with column_type = 'Super';");
        }

        keyspace = configureHectorAccessToCassandra(cassandraHostname, cassandraPort, cassandraClusterName,
                cassandraKeySpaceName, configurationPath, cassandraCommands);
    }

    @Test
    public void testHectorAccess() throws Exception {

        HectorHeterogeneousSuperColumnExample example = new HectorHeterogeneousSuperColumnExample(keyspace, columnFamilyName);

        runAccessorTest(example);
    }
}
