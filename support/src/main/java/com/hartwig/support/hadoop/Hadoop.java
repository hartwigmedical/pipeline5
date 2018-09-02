package com.hartwig.support.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public class Hadoop {

    public static FileSystem fileSystem(final String url) throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", url);
        return FileSystem.get(conf);
    }

    public static FileSystem localFilesystem() throws IOException {
        return fileSystem("file:///");
    }
}
