package com.hartwig.pipeline.cram;

import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.io.File;

@Value.Immutable
public interface CramOutput extends StageOutput {
    static String cramFile(String inputBam) {
        return new File(inputBam.replaceAll("\\.bam$", ".cram")).getName();
    }

    static String craiFile(String cram) {
        return cram + ".crai";
    }

    static ImmutableCramOutput.Builder builder() {
        return ImmutableCramOutput.builder();
    }

    @Override
    default String name() {
        return CramConversion.NAMESPACE;
    }
}
