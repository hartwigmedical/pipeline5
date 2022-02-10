package com.hartwig.pipeline.calling.structural.gripss;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssGermlineProcessOutput extends StageOutput {

    @Override
    default String name() {
        return GripssGermlineProcess.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeFilteredVcf();

    Optional<GoogleStorageLocation> maybeFilteredVcfIndex();

    Optional<GoogleStorageLocation> maybeFullVcf();

    Optional<GoogleStorageLocation> maybeFullVcfIndex();

    default GoogleStorageLocation filteredVcf() {
        return maybeFilteredVcf().orElseThrow(() -> new IllegalStateException("No filtered VCF available"));
    }

    default GoogleStorageLocation fullVcf() {
        return maybeFullVcf().orElseThrow(() -> new IllegalStateException("No full VCF available"));
    }

    default GoogleStorageLocation filteredVcfIndex() {
        return maybeFilteredVcfIndex().orElseThrow(() -> new IllegalStateException("No filtered VCF index available"));
    }

    default GoogleStorageLocation fullVcfIndex() {
        return maybeFullVcfIndex().orElseThrow(() -> new IllegalStateException("No full VCF index available"));
    }

    static ImmutableGripssGermlineProcessOutput.Builder builder() {
        return ImmutableGripssGermlineProcessOutput.builder();
    }
}
