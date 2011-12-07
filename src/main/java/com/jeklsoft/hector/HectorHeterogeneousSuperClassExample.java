package com.jeklsoft.hector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
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
Cluster: Education

Keyspace: "Schools" {
    CF<super column>: Courses {
      school_uuid : {
        Courses : {
          course_uuid : {
            <start date> : {
              "CourseName" : <String, "course name">,
              "numberEnrolled" : <Long, value>,
              "graduateLevel" : <Boolean, 1/0>,
              "passRate" : <Double, percent passed>,
              "meetingTime" : <Date, time of class, e.g., 8 AM>
            }

            ...

            <start date> : {
              "CourseName" : <String, "course name">,
              "numberEnrolled" : <Long, value>,
              "graduateLevel" : <Boolean, 1/0>,
              "passRate" : <Double, percent passed>,
              "meetingTime" : <Date, time of class, e.g., 8 AM>
            }
          }

          ...

          course_uuid : {
            ...
          }
      }
    }
}
*/

public class HectorHeterogeneousSuperClassExample {

    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperClassExample.class);

    private static final String embeddedCassandraHostname = "localhost";
    private static final Integer embeddedCassandraPort = 9160;
    private static final String embeddedCassandraKeySpaceName = "Schools"; //new UUID(0,255).toString().replace("-","");
    private static final String embeddedCassandraClusterName = "Education";
    private static final String columnFamilyName = "Courses";
    private static final String configurationPath = "/tmp/cassandra";
    private static final Serializer genericOutputSerializer = ExtendedTypeInferringSerializer.get();
    private static final Serializer ss = StringSerializer.get();
    private static final Serializer us = UUIDSerializer.get();
    private static final String courseNameColumnName = "CourseName";
    private static final String numberEnrolledColumnName = "NumberEnrolled";
    private static final String graduateLevelColumnName = "GraduateLevel";
    private static final String passRateColumnName = "PassRate";
    private static final String meetingTimeColumnName = "MeetingTime";

    private final Keyspace keyspace;

    public HectorHeterogeneousSuperClassExample()
    {
        try
        {
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

    public void addCourse(Course course)
    {
        List<Course> courses = new ArrayList<Course>();
        courses.add(course);

        addCourses(courses);
    }

    public void addCourses(List<Course> courses)
    {
        Mutator mutator = HFactory.createMutator(keyspace, genericOutputSerializer);

        for (Course course : courses)
        {
            HColumn courseNameColumn = HFactory.createColumn(courseNameColumnName,
                    course.getCourseName(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn numberEnrolledColumn = HFactory.createColumn(numberEnrolledColumnName,
                    course.getEnrollmentCount(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn graduateLevelColumn = HFactory.createColumn(graduateLevelColumnName,
                    course.isGraduateLevel(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn passRateColumn = HFactory.createColumn(passRateColumnName,
                    course.getPassRate(),
                    genericOutputSerializer,
                    genericOutputSerializer);
            HColumn meetingTimeColumn = HFactory.createColumn(meetingTimeColumnName,
                    course.getMeetingTime(),
                    genericOutputSerializer,
                    genericOutputSerializer);

            List columnList = new ArrayList();
            columnList.add(courseNameColumn);
            columnList.add(numberEnrolledColumn);
            columnList.add(graduateLevelColumn);
            columnList.add(passRateColumn);
            columnList.add(meetingTimeColumn);

            HSuperColumn superColumn = HFactory.createSuperColumn(course.getCourseId(),
                    columnList,
                    genericOutputSerializer,
                    genericOutputSerializer,
                    genericOutputSerializer);

            mutator.addInsertion(course.getSchoolId(), columnFamilyName, superColumn);
        }

        mutator.execute();
    }

    public List<Course> queryForCourseListById(UUID schoolId, UUID courseLowerRange, UUID courseUpperRange, int maxToReturn)
    {
        SuperSliceQuery query = HFactory.createSuperSliceQuery(keyspace, us, us, ss, ByteBufferSerializer.get());

        query.setColumnFamily(columnFamilyName).setKey(schoolId).setRange(courseLowerRange, courseUpperRange, false, maxToReturn);

        QueryResult<SuperSlice<UUID, String, ByteBuffer>> result = query.execute();

        List<HSuperColumn<UUID, String, ByteBuffer>> rows = result.get().getSuperColumns();

        List<Course> courses = new ArrayList<Course>();

        for (HSuperColumn row : rows)
        {
            Course course = getCourseFromSuperColumn(schoolId, row);
            courses.add(course);
        }
        return courses;
    }

    private Course getCourseFromSuperColumn(UUID schoolId, HSuperColumn row)
    {
        final String courseNameColumnName = "CourseName";
        final String numberEnrolledColumnName = "NumberEnrolled";
        final String graduateLevelColumnName = "GraduateLevel";
        final String passRateColumnName = "PassRate";
        final String meetingTimeColumnName = "MeetingTime";

        UUID courseId = (UUID)row.getName();
        String courseName = null;
        Long enrollmentCount = null;
        Boolean graduateLevel = null;
        Double passRate = null;
        Date meetingTime = null;

        List<HColumn<String, ByteBuffer>> columns = row.getColumns();
        for(HColumn<String, ByteBuffer> column : columns)
        {
            if (courseNameColumnName.equals(column.getName()))
            {
                courseName = StringSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (numberEnrolledColumnName.equals(column.getName()))
            {
                enrollmentCount = LongSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (graduateLevelColumnName.equals(column.getName()))
            {
                graduateLevel = BooleanSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (passRateColumnName.equals(column.getName()))
            {
                passRate = DoubleSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (meetingTimeColumnName.equals(column.getName()))
            {
                meetingTime = DateSerializer.get().fromByteBuffer(column.getValue());
            }
            else
            {
                throw new RuntimeException("Unknown column name " + column.getName());
            }
        }

        if ((courseId == null) || (courseName == null) ||
            (enrollmentCount == null) || (graduateLevel == null) ||
            (passRate == null) || (meetingTime == null))
        {
            throw new RuntimeException("Missing Columns");
        }

        Course course = new Course(schoolId, courseId, courseName, enrollmentCount, graduateLevel, passRate, meetingTime);
        return course;
    }
}
