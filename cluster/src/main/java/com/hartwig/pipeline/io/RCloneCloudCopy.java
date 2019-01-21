package com.hartwig.pipeline.io;

import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.after.Processes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RCloneCloudCopy implements CloudCopy {

    private static final Logger LOGGER = LoggerFactory.getLogger(RCloneCloudCopy.class);
    static final String RCLONE = "/rclone";

    private final String rClonePath;
    private final String gcpRemote;
    private final String s3Remote;
    private final Supplier<ProcessBuilder> processBuilderSupplier;

    public RCloneCloudCopy(final String rClonePath, final String gcpRemote, final String s3Remote,
            final Supplier<ProcessBuilder> processBuilder) {
        this.rClonePath = rClonePath;
        this.gcpRemote = gcpRemote;
        this.s3Remote = s3Remote;
        this.processBuilderSupplier = processBuilder;
    }

    @Override
    public void copy(final String from, final String to) {
        try {
            ProcessBuilder processBuilder = processBuilderSupplier.get();
            List<String> command = ImmutableList.of(rClonePath + RCLONE, "copyto", replaceRemotes(from), replaceRemotes(to), "-vv");
            processBuilder.command(command);
            LOGGER.info("Running rclone command [{}]", command.stream().collect(joining(" ")));
            Processes.run(processBuilder, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String replaceRemotes(final String from) {
        return from.replace("gs://", gcpRemote + ":").replace("s3://", s3Remote + ":");
    }
}
