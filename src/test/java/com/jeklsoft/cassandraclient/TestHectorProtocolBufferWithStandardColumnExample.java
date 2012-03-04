package com.jeklsoft.cassandraclient;

import me.prettyprint.hector.api.Keyspace;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestHectorProtocolBufferWithStandardColumnExample extends BaseReadingsTest {
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
            cassandraCommands.add("create column family " + columnFamilyName + ";");
        }

        keyspace = TestUtils.configureHectorAccessToCassandra(cassandraHostname, cassandraPort, cassandraClusterName,
                cassandraKeySpaceName, configurationPath, cassandraCommands);
    }

    @Test
    public void testHectorAccess() throws Exception {
        HectorProtocolBufferWithStandardColumnExample example = new HectorProtocolBufferWithStandardColumnExample(keyspace, columnFamilyName);

        runAccessorTest(example);
    }
}
