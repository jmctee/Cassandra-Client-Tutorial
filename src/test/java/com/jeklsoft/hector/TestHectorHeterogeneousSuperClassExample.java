package com.jeklsoft.hector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class TestHectorHeterogeneousSuperClassExample {

    @Test
    public void testHectorAccess() throws Exception {
        HectorHeterogeneousSuperClassExample example = new HectorHeterogeneousSuperClassExample();

        List<Course> courses = new ArrayList<Course>();
        courses.add(new Course(new UUID(0,100), new UUID(0,1), "Basket Weaving", new Long(25), false, new Double(0.17), new Date(0,0,1,8,0)));
        courses.add(new Course(new UUID(0,100), new UUID(0,2), "Walking And Chewing Gum", new Long(23), false, new Double(0.45), new Date(0,0,1,14,30)));
        courses.add(new Course(new UUID(0,100), new UUID(0,3), "Navel Gazing", new Long(2), true, new Double(0.50), new Date(0,0,1,12,0)));
        courses.add(new Course(new UUID(0,100), new UUID(0,4), "Staring Blankly", new Long(0), true, new Double(0.0), new Date(0,0,1,13,0)));

        courses.add(new Course(new UUID(0,200), new UUID(0,1), "Basket Weaving", new Long(25), false, new Double(0.17), new Date(0,0,1,8,0)));
        courses.add(new Course(new UUID(0,200), new UUID(0,2), "Walking And Chewing Gum", new Long(23), false, new Double(0.45), new Date(0,0,1,14,30)));
        courses.add(new Course(new UUID(0,200), new UUID(0,3), "Navel Gazing", new Long(2), true, new Double(0.50), new Date(0,0,1,12,0)));
        courses.add(new Course(new UUID(0,200), new UUID(0,4), "Staring Blankly", new Long(0), true, new Double(0.0), new Date(0,0,1,13,0)));

        List<Course> expectedCourses = new ArrayList<Course>();
        expectedCourses.add(new Course(new UUID(0,100), new UUID(0,2), "Walking And Chewing Gum", new Long(23), false, new Double(0.45), new Date(0,0,1,14,30)));
        expectedCourses.add(new Course(new UUID(0,100), new UUID(0,3), "Navel Gazing", new Long(2), true, new Double(0.50), new Date(0,0,1,12,0)));

        example.addCourses(courses);

        List<Course> returnedCourses = example.queryForCourseListById(new UUID(0,100), new UUID(0,2), new UUID(0,3), 10);

        assertEquals(expectedCourses.size(), returnedCourses.size());

        for (Course expectedCourse : expectedCourses)
        {
            assertTrue(returnedCourses.contains(expectedCourse));
        }
    }
}
