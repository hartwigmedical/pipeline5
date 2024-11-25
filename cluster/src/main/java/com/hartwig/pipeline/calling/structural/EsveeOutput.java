package com.hartwig.pipeline.calling.structural;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface EsveeOutput extends StageOutput {

    @Override
    default String name() {
        return Esvee.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybePrepBam();
    Optional<GoogleStorageLocation> maybePrepBamIndex();
    Optional<GoogleStorageLocation> maybePrepJunctionTsv();
    Optional<GoogleStorageLocation> maybeFragLengths();
    Optional<GoogleStorageLocation> maybeDiscStats();

    Optional<GoogleStorageLocation> maybeAssemblyTsv();
    Optional<GoogleStorageLocation> maybePhasedAssemblyTsv();
    Optional<GoogleStorageLocation> maybeBreakendTsv();
    Optional<GoogleStorageLocation> maybeAlignmentTsv();

    Optional<GoogleStorageLocation> maybeUnfilteredVcf();
    Optional<GoogleStorageLocation> maybeSomaticVcf();
    Optional<GoogleStorageLocation> maybeGermlineVcf();

    default GoogleStorageLocation prepBam() {
        return maybePrepBam().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation prepBamIndex() {
        return maybePrepBamIndex().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation prepJunctions() {
        return maybePrepJunctionTsv().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation fragmentLengths() { return maybeFragLengths().orElse(GoogleStorageLocation.empty()); }
    default GoogleStorageLocation discordantStatistics() { return maybeDiscStats().orElse(GoogleStorageLocation.empty()); }
    default GoogleStorageLocation assemblyTsv() {
        return maybeAssemblyTsv().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation phasedAssemblyTsv() {
        return maybePhasedAssemblyTsv().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation breakendTsv() {
        return maybeBreakendTsv().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation alignmentTsv() {
        return maybeAlignmentTsv().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation unfilteredVcf() {
        return maybeUnfilteredVcf().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation somaticVcf() {
        return maybeSomaticVcf().orElse(GoogleStorageLocation.empty());
    }
    default GoogleStorageLocation germlineVcf() {
        return maybeGermlineVcf().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableEsveeOutput.Builder builder() {
        return ImmutableEsveeOutput.builder();
    }
}
