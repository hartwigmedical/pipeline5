package com.hartwig.pipeline.execution.vm;

import java.io.File;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface OutputFile {

    @Value.Parameter
    String fileName();

    default String path() {
        return VmDirectories.OUTPUT + "/" + fileName();
    }

    default OutputFile index(final String suffix) {
        return ImmutableOutputFile.builder().from(this).fileName(fileName() + suffix).build();
    }

    static OutputFile of(final String sample, final String subStageName, final String type) {
        return ImmutableOutputFile.of(String.format("%s.%s.%s", sample, subStageName, type));
    }

    static OutputFile of(final String sample, final String type) {
        return ImmutableOutputFile.of(String.format("%s.%s", sample, type));
    }

    static OutputFile empty() {
        return ImmutableOutputFile.of("not.a.file");
    }
}
