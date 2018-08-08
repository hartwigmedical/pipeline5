package com.hartwig.pipeline.runtime.hadoop;

import java.io.IOException;

import com.hartwig.pipeline.runtime.configuration.Configuration;

import org.apache.hadoop.fs.FileSystem;

public class Hadoop {

    public static FileSystem fileSystem(Configuration configuration) throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", configuration.pipeline().hdfs());
        return FileSystem.get(conf);
    }
}
