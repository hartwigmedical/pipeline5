package com.hartwig.support.test;

public class Resources {

    public static String testResource(String name) {
        return System.getProperty("user.dir") + "/src/test/resources/" + name;
    }

    public static String targetResource(String name) {
        return System.getProperty("user.dir") + "/target/" + name;
    }
}
