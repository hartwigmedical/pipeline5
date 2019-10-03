package com.hartwig.pipeline.calling.germline;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GermlineCallerOutput extends StageOutput{

    @Override
    default String name() {
        return GermlineCaller.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeGermlineVcfLocation();

    default GoogleStorageLocation germlineVcfLocation() {
        return maybeGermlineVcfLocation().orElseThrow(() -> new IllegalStateException("No germline VCF available"));
    }

    static ImmutableGermlineCallerOutput.Builder builder() {
        return ImmutableGermlineCallerOutput.builder();
    }
}