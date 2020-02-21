package com.hartwig.pipeline.cram;

import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

@Value.Immutable
public interface CramOutput extends StageOutput {
    static String cramFile(String sample) {
        return sample.replaceAll("\\.bam$", ".cram");
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
