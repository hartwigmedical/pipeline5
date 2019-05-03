package com.hartwig.pipeline.execution.vm;

import org.immutables.value.Value;

@Value.Immutable
public interface OutputFile {

    String VCF = "vcf";
    String GZIPPED_VCF = VCF + ".gz";

    @Value.Parameter
    String path();

    static OutputFile of(String sample, String step, String type) {
        return ImmutableOutputFile.of(String.format("%s/%s.%s.%s", VmDirectories.OUTPUT, sample, step, type));
    }

    static OutputFile of(String sample, String type) {
        return ImmutableOutputFile.of(String.format("%s/%s.%s", VmDirectories.OUTPUT, sample, type));
    }

    static OutputFile empty() {
        return ImmutableOutputFile.of("not.a.file");
    }
}
