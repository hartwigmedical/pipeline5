package com.hartwig.pipeline;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopStatusReporter implements StatusReporter {

    private final Logger LOGGER = LoggerFactory.getLogger(StatusReporter.class);
    private final FileSystem fileSystem;
    private final String bamFolder;

    public HadoopStatusReporter(final FileSystem fileSystem, final String bamFolder) {
        this.fileSystem = fileSystem;
        this.bamFolder = bamFolder;
    }

    public void report(final StatusReporter.Status status) {
        try {
            deletePreviousStatus();
            try (OutputStream stream = outputStream(status)) {
                stream.write(status.toString().getBytes());
            }
        } catch (IOException e) {
            LOGGER.error("Unable to write final status of this pipeline run. This will cause downstream issues in identifying whether this "
                    + "BAM can be used for further processing.");
        }
    }

    private void deletePreviousStatus() throws IOException {
        fileSystem.delete(filePath(SUCCESS), false);
        fileSystem.delete(filePath(FAILURE), false);
    }

    private FSDataOutputStream outputStream(Status status) throws IOException {
        String fileName = status == Status.SUCCESS ? SUCCESS : FAILURE;
        return fileSystem.create(filePath(fileName));
    }

    @NotNull
    private Path filePath(final String fileName) {
        return new Path(fileSystem.getUri() + bamFolder + fileName);
    }
}
