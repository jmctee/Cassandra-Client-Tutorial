package com.jeklsoft.hector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHectorHeterogeneousSuperClassExample {

    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperClassExample.class);

    private static final String configurationPath = "/tmp/cassandra";
    private static final String cassandraHostname = "localhost";
    private static final Integer cassandraPort = 9160;
    private static final String cassandraKeySpaceName = "Climate";
    private static final String cassandraClusterName = "SensorNet";
    private static final String columnFamilyName = "BoulderSensors";

    private static Keyspace keyspace;

    @BeforeClass
    public static void configureCassandra() throws Exception {

        try
        {
            initializeEmbeddedCassandra();

            CassandraHostConfigurator configurator = new CassandraHostConfigurator(cassandraHostname + ":" + cassandraPort);
            Cluster cluster = HFactory.getOrCreateCluster(cassandraClusterName, configurator);
            keyspace = HFactory.createKeyspace(cassandraKeySpaceName, cluster);
        }
        catch (Exception e)
        {
            log.log(Level.ERROR,"Error received",e);
            throw new RuntimeException(e.getMessage());
        }
    }

    private static void initializeEmbeddedCassandra() throws Exception {

        FileUtils.deleteDirectory(new File(configurationPath));

        URL stream = TestHectorHeterogeneousSuperClassExample.class.getClassLoader().getResource("cassandra.yaml");
        File cassandraYaml = new File(stream.toURI());

        FileUtils.copyFileToDirectory(cassandraYaml, new File(configurationPath));

        List<String> cassandraCommands = new ArrayList<String>();
        cassandraCommands.add("create keyspace " + cassandraKeySpaceName + ";");
        cassandraCommands.add("use " + cassandraKeySpaceName + ";");
        cassandraCommands.add("create column family " + columnFamilyName + " with column_type = 'Super';");

        EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
        embeddedCassandra.setCleanCassandra(true);
        embeddedCassandra.setCassandraStartupCommands(cassandraCommands);
        embeddedCassandra.setHostname(cassandraHostname);
        embeddedCassandra.setHostport(cassandraPort);
        embeddedCassandra.setCassandraConfigDirPath(configurationPath);
        embeddedCassandra.init();
    }

    @Test
    public void testHectorAccess() throws Exception {

        HectorHeterogeneousSuperClassExample example = new HectorHeterogeneousSuperClassExample(keyspace, columnFamilyName);

        UUID sensorId1 = new UUID(0,100);
        UUID sensorId2 = new UUID(0,200);

        DateTime baseDate = new DateTime();
        int readingInterval = 15; // 15 minutes

        List<Reading> readings = new ArrayList<Reading>();

        for (int ii=0; ii<10; ii++) {

            BigDecimal temperature = new BigDecimal(230 + ii).movePointLeft(1);
            readings.add(new Reading(sensorId1, baseDate.plusMinutes(readingInterval * ii), temperature,
                         16, "W", BigInteger.valueOf(17L), false));

            temperature = new BigDecimal(195 - ii).movePointLeft(1);
            readings.add(new Reading(sensorId2, baseDate.plusMinutes(readingInterval * ii), temperature,
                         24, "ESE", BigInteger.valueOf(17L), false));
        }

        example.addReadings(readings);

        List<Reading> expectedReadings = new ArrayList<Reading>();
        for (int ii=0; ii<3; ii++) {
            BigDecimal temperature = new BigDecimal(234 + ii).movePointLeft(1);
            expectedReadings.add(new Reading(sensorId1, baseDate.plusMinutes(readingInterval * (ii + 4)),
                                             temperature, 16, "W", BigInteger.valueOf(17L), false));
        }

        DateTime startTime = new DateTime(baseDate.plusMinutes(readingInterval * 4));
        DateTime endTime = new DateTime(baseDate.plusMinutes(readingInterval * 6));
        Duration duration = new Duration(startTime, endTime);
        Interval interval = new Interval(startTime, duration);

        List<Reading> returnedReadings = example.querySensorReadingsByInterval(sensorId1, interval, 10);

        assertEquals(expectedReadings.size(), returnedReadings.size());

        for (Reading expectedReading : expectedReadings)
        {
            assertTrue(returnedReadings.contains(expectedReading));
        }

        expectedReadings = new ArrayList<Reading>();
        for (int ii=0; ii<4; ii++) {
            BigDecimal temperature = new BigDecimal(195 - ii).movePointLeft(1);
            expectedReadings.add(new Reading(sensorId2, baseDate.plusMinutes(readingInterval * ii),
                                             temperature, 24, "ESE", BigInteger.valueOf(17L), false));
        }

        startTime = new DateTime(baseDate);
        endTime = new DateTime(baseDate.plusMinutes(readingInterval * 3));
        duration = new Duration(startTime, endTime);
        interval = new Interval(startTime, duration);

        returnedReadings = example.querySensorReadingsByInterval(sensorId2, interval, 10);

        assertEquals(expectedReadings.size(), returnedReadings.size());

        for (Reading expectedReading : expectedReadings)
        {
            assertTrue(returnedReadings.contains(expectedReading));
        }
    }
}
