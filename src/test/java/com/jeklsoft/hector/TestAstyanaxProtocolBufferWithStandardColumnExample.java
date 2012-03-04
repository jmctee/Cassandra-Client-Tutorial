package com.jeklsoft.hector;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestAstyanaxProtocolBufferWithStandardColumnExample extends BaseReadingsTest {
    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperColumnExample.class);

    private static final Boolean useEmbeddedCassandra = true;

    @BeforeClass
    public static void configureCassandra() throws Exception {
        List<String> cassandraCommands = null;

        if (useEmbeddedCassandra) {
            cassandraCommands = new ArrayList<String>();
            cassandraCommands.add("create keyspace " + cassandraKeySpaceName + ";");
            cassandraCommands.add("use " + cassandraKeySpaceName + ";");
            cassandraCommands.add("create column family " + columnFamilyName + ";");
        }
    }

    @Test
    public void testAstyanaxAccess() {

        AstyanaxProtocolBufferWithStandardColumnExample example = new AstyanaxProtocolBufferWithStandardColumnExample();

//        runAccessorTest(example);
    }
}
