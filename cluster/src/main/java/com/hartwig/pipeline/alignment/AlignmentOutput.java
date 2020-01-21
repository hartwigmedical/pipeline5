package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.alignment.vm.VmAligner;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import org.immutables.value.Value;

import java.util.Optional;
import java.util.function.Supplier;

@Value.Immutable
public interface AlignmentOutput extends StageOutput {

    String sample();

    default String name() {
        return VmAligner.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeFinalBamLocation();

    Optional<GoogleStorageLocation> maybeFinalBaiLocation();

    default GoogleStorageLocation finalBamLocation() {
        return maybeFinalBamLocation().orElseThrow(noBamAvailable());
    }

    default GoogleStorageLocation finalBaiLocation() {
        return maybeFinalBaiLocation().orElseThrow(noBaiAvailable());
    }

    static Supplier<IllegalStateException> noBamAvailable() {
        return () -> new IllegalStateException("No BAM available");
    }

    static Supplier<IllegalStateException> noBaiAvailable() {
        return () -> new IllegalStateException("No BAI available");
    }

    static ImmutableAlignmentOutput.Builder builder() {
        return ImmutableAlignmentOutput.builder();
    }
}
