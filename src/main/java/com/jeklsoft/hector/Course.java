package com.jeklsoft.hector;

import java.util.Date;
import java.util.UUID;

public class Course {
    private UUID schoolId;
    private UUID courseId;
    private String courseName;
    private Long enrollmentCount;
    private Boolean graduateLevel;
    private Double passRate;
    private Date meetingTime;

    public Course(UUID schoolId, UUID courseId, String courseName, Long enrollmentCount, Boolean graduateLevel,
                  Double passRate, Date meetingTime)
    {
        this.schoolId = schoolId;
        this.courseId = courseId;
        this.courseName = courseName;
        this.enrollmentCount = enrollmentCount;
        this.graduateLevel = graduateLevel;
        this.passRate = passRate;
        this.meetingTime = meetingTime;
    }

    public UUID getSchoolId() {
        return schoolId;
    }

    public UUID getCourseId() {
        return courseId;
    }

    public String getCourseName() {
        return courseName;
    }


    public Long getEnrollmentCount() {
        return enrollmentCount;
    }


    public Boolean isGraduateLevel() {
        return graduateLevel;
    }

    public Double getPassRate() {
        return passRate;
    }

    public Date getMeetingTime() {
        return meetingTime;
    }

    @Override
    public String toString() {
        return "Course{" +
                "schoolId=" + schoolId +
                ", courseId=" + courseId +
                ", courseName='" + courseName + '\'' +
                ", enrollmentCount=" + enrollmentCount +
                ", graduateLevel=" + graduateLevel +
                ", passRate=" + passRate +
                ", meetingTime=" + meetingTime.getHours() + ":" + meetingTime.getMinutes() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Course course = (Course) o;

        if (courseId != null ? !courseId.equals(course.courseId) : course.courseId != null) return false;
        if (courseName != null ? !courseName.equals(course.courseName) : course.courseName != null) return false;
        if (enrollmentCount != null ? !enrollmentCount.equals(course.enrollmentCount) : course.enrollmentCount != null)
            return false;
        if (graduateLevel != null ? !graduateLevel.equals(course.graduateLevel) : course.graduateLevel != null)
            return false;
        if (meetingTime != null ? !meetingTime.equals(course.meetingTime) : course.meetingTime != null)
            return false;
        if (passRate != null ? !passRate.equals(course.passRate) : course.passRate != null) return false;
        if (schoolId != null ? !schoolId.equals(course.schoolId) : course.schoolId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = schoolId != null ? schoolId.hashCode() : 0;
        result = 31 * result + (courseId != null ? courseId.hashCode() : 0);
        result = 31 * result + (courseName != null ? courseName.hashCode() : 0);
        result = 31 * result + (enrollmentCount != null ? enrollmentCount.hashCode() : 0);
        result = 31 * result + (graduateLevel != null ? graduateLevel.hashCode() : 0);
        result = 31 * result + (passRate != null ? passRate.hashCode() : 0);
        result = 31 * result + (meetingTime != null ? meetingTime.hashCode() : 0);
        return result;
    }
}
