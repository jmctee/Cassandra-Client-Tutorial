package com.jeklsoft.hector;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperSliceQuery;

/*
Cluster: SensorNet

Keyspace: "Climate" {
    CF<supercolumn>: "Sensors" {
        sensor_uuid: {
            <timestamp> : {
                "Temperature" : <Double>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }

            ...

            <timestamp> : {
                "Temperature" : <Double>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }
        }

        ...

        sensor_uuid: {
            <timestamp> : {
                "Temperature" : <Double>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }

            ...

            <timestamp> : {
                "Temperature" : <Double>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }
        }
    }
}
*/

public class HectorHeterogeneousSuperClassExample {

    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperClassExample.class);

    private static final String configurationPath = "/tmp/cassandra";
    private static final String embeddedCassandraHostname = "localhost";
    private static final Integer embeddedCassandraPort = 9160;
    private static final String embeddedCassandraKeySpaceName = "Climate";
    private static final String embeddedCassandraClusterName = "SensorNet";
    private static final String columnFamilyName = "Sensors";

    private static final Serializer genericOutputSerializer = ExtendedTypeInferringSerializer.get();
    private static final Serializer ss = StringSerializer.get();
    private static final Serializer us = UUIDSerializer.get();
    private static final Serializer ls = LongSerializer.get();

    private static final String temperatureNameColumnName = "Temperature";
    private static final String windSpeedNameColumnName = "WindSpeed";
    private static final String windDirectionNameColumnName = "WindDirection";
    private static final String humidityNameColumnName = "Humidity";
    private static final String badAirQualityDetectedNameColumnName = "BadAirQualityDetected";
    
    private final Keyspace keyspace;

    public HectorHeterogeneousSuperClassExample()
    {
        try
        {
            List<String> cassandraCommands = new ArrayList<String>();
            cassandraCommands.add("create keyspace " + embeddedCassandraKeySpaceName + ";");
            cassandraCommands.add("use " + embeddedCassandraKeySpaceName + ";");
//            cassandraCommands.add("create column family " + columnFamilyName + " with column_type = 'Super' and comparator = 'LongType';");
            cassandraCommands.add("create column family " + columnFamilyName + " with column_type = 'Super';");

            EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
            embeddedCassandra.setCleanCassandra(true);
            embeddedCassandra.setCassandraStartupCommands(cassandraCommands);
            embeddedCassandra.setHostname(embeddedCassandraHostname);
            embeddedCassandra.setHostport(embeddedCassandraPort);
            embeddedCassandra.setCassandraConfigDirPath(configurationPath);
            embeddedCassandra.init();

            CassandraHostConfigurator configurator = new CassandraHostConfigurator(embeddedCassandraHostname + ":" + embeddedCassandraPort);
            Cluster cluster = HFactory.getOrCreateCluster(embeddedCassandraClusterName, configurator);
            keyspace = HFactory.createKeyspace(embeddedCassandraKeySpaceName, cluster);
        }
        catch (Exception e)
        {
            log.log(Level.ERROR,"Error received",e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public void addReading(final Reading reading)
    {
        List<Reading> readings = new ArrayList<Reading>();
        readings.add(reading);

        addReadings(readings);
    }

    public void addReadings(final List<Reading> readings)
    {
        Mutator mutator = HFactory.createMutator(keyspace, genericOutputSerializer);

        for (Reading reading : readings)
        {
            HColumn temperatureColumn = HFactory.createColumn(temperatureNameColumnName,
                    reading.getTemperature(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn windSpeedColumn = HFactory.createColumn(windSpeedNameColumnName,
                    reading.getWindSpeed(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn windDirectionColumn = HFactory.createColumn(windDirectionNameColumnName,
                    reading.getDirection(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn humidityColumn = HFactory.createColumn(humidityNameColumnName,
                    reading.getHumidity(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn badAirQualityDetectedColumn = HFactory.createColumn(badAirQualityDetectedNameColumnName,
                    reading.getBadAirQualityDetected(),
                    genericOutputSerializer,
                    genericOutputSerializer);

            List columnList = new ArrayList();
            columnList.add(temperatureColumn);
            columnList.add(windSpeedColumn);
            columnList.add(windDirectionColumn);
            columnList.add(humidityColumn);
            columnList.add(badAirQualityDetectedColumn);

            HSuperColumn superColumn = HFactory.createSuperColumn(reading.getTimestamp().getTime(),
                    columnList,
                    genericOutputSerializer,
                    genericOutputSerializer,
                    genericOutputSerializer);

            mutator.addInsertion(reading.getSensorId(), columnFamilyName, superColumn);
        }

        mutator.execute();
    }

    public List<Reading> querySensorReadingsByTime(UUID sensorId, Date startingDate, Date endingDate, int maxToReturn)
    {
        SuperSliceQuery query = HFactory.createSuperSliceQuery(keyspace, us, ls, ss, ByteBufferSerializer.get());

        query.setColumnFamily(columnFamilyName).setKey(sensorId).setRange(startingDate.getTime(), endingDate.getTime(), false, maxToReturn);

        QueryResult<SuperSlice<UUID, String, ByteBuffer>> result = query.execute();

        List<HSuperColumn<UUID, String, ByteBuffer>> rows = result.get().getSuperColumns();

        List<Reading> readings = new ArrayList<Reading>();

        for (HSuperColumn row : rows)
        {
            Reading reading = getReadingFromSuperColumn(sensorId, row);
            readings.add(reading);
        }
        return readings;
    }

    private Reading getReadingFromSuperColumn(UUID sensorId, HSuperColumn row)
    {
        Date timestamp = new Date((Long)row.getName());
        Double temperature = null;
        Integer windSpeed = null;
        String direction = null;
        BigInteger humidity = null;
        Boolean badAirQualityDetected = null;

        List<HColumn<String, ByteBuffer>> columns = row.getColumns();
        for(HColumn<String, ByteBuffer> column : columns)
        {
            if (temperatureNameColumnName.equals(column.getName()))
            {
                temperature = DoubleSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (windSpeedNameColumnName.equals(column.getName()))
            {
                windSpeed = IntegerSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (windDirectionNameColumnName.equals(column.getName()))
            {
                direction = StringSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (humidityNameColumnName.equals(column.getName()))
            {
                humidity = BigIntegerSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (badAirQualityDetectedNameColumnName.equals(column.getName()))
            {
                badAirQualityDetected = BooleanSerializer.get().fromByteBuffer(column.getValue());
            }
            else
            {
                throw new RuntimeException("Unknown column name " + column.getName());
            }
        }

        // TODO: use assertion here
        if ((temperature == null) || (windSpeed == null) ||
            (direction == null) || (humidity == null) ||
            (badAirQualityDetected == null))
        {
            throw new RuntimeException("Missing Columns");
        }

        Reading reading = new Reading(sensorId, timestamp, temperature, windSpeed, direction, humidity, badAirQualityDetected);
        return reading;
    }
}
