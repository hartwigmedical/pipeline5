package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GermlineCallerOutput {

    @Value.Parameter
    GoogleStorageLocation germlineVcf();

    static GermlineCallerOutput of(String bucket, String vcfLocation) {
        return ImmutableGermlineCallerOutput.of(GoogleStorageLocation.of(bucket, vcfLocation));
    }
}
