package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface LilacBamSliceOutput extends StageOutput {
    @Override
    default String name() {
        return LilacBamSlicer.NAMESPACE;
    }

    Optional<GoogleStorageLocation> reference();

    Optional<GoogleStorageLocation> referenceIndex();

    Optional<GoogleStorageLocation> tumor();

    Optional<GoogleStorageLocation> tumorIndex();

    static ImmutableLilacBamSliceOutput.Builder builder() {
        return ImmutableLilacBamSliceOutput.builder();
    }
}
