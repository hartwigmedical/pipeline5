package com.hartwig.pipeline.adam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hartwig.patient.ReferenceGenome;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;

class IndexFiles {

    static List<String> resolve(FileSystem fileSystem, ReferenceGenome referenceGenome) {
        try {
            int lastSlash = referenceGenome.path().lastIndexOf("/");
            String directory = referenceGenome.path().substring(0, lastSlash);
            String file = referenceGenome.path().substring(lastSlash + 1);
            FileStatus[] statuses = fileSystem.listStatus(new Path(directory), new GlobFilter(file + ".*"));
            List<String> files = new ArrayList<>();
            files.add(referenceGenome.path());
            for (FileStatus status : statuses) {
                files.add(status.getPath().toString());
            }
            return files;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
