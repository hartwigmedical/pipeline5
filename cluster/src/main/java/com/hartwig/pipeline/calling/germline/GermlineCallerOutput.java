package com.hartwig.pipeline.calling.germline;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GermlineCallerOutput extends StageOutput {

    @Override
    default String name() {
        return GermlineCaller.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeGermlineVcfLocation();

    Optional<GoogleStorageLocation> maybeGermlineVcfIndexLocation();

    default GoogleStorageLocation germlineVcfLocation() {
        return maybeGermlineVcfLocation().orElseThrow(() -> new IllegalStateException("No germline VCF available"));
    }

    default GoogleStorageLocation germlineVcfIndexLocation() {
        return maybeGermlineVcfIndexLocation().orElseThrow(() -> new IllegalStateException("No germline VCF index available"));
    }

    static ImmutableGermlineCallerOutput.Builder builder() {
        return ImmutableGermlineCallerOutput.builder();
    }

    static OutputFile outputFile(final String sample) {
        return OutputFile.of(sample, "germline", FileTypes.GZIPPED_VCF);
    }
}