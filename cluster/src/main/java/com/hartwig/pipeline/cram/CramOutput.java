package com.hartwig.pipeline.cram;

import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.io.File;

@Value.Immutable
public interface CramOutput extends StageOutput {
    static String cramFile(String sample) {
        return new File(sample.replaceAll("\\.bam$", ".cram")).getName();
    }

    static String craiFile(String sample) {
        return cramFile(sample) + ".crai";
    }

    static ImmutableCramOutput.Builder builder() {
        return ImmutableCramOutput.builder();
    }

    @Override
    default String name() {
        return CramConversion.NAMESPACE;
    }
}
