package com.hartwig.pipeline.execution.vm;

import org.immutables.value.Value;

@Value.Immutable
public interface OutputFile {

    String VCF = "vcf";
    String GZIPPED_VCF = VCF + ".gz";

    @Value.Parameter
    String fileName();

    default String path() {
        return VmDirectories.OUTPUT + "/" + fileName();
    }

    static OutputFile of(String sample, String step, String type) {
        return ImmutableOutputFile.of(String.format("%s.%s.%s", sample, step, type));
    }

    static OutputFile of(String sample, String type) {
        return ImmutableOutputFile.of(String.format("%s.%s", sample, type));
    }

    static OutputFile empty() {
        return ImmutableOutputFile.of("not.a.file");
    }
}
