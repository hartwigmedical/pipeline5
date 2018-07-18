package com.hartwig.pipeline.runtime.configuration;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePatientParameters.class)
@Value.Immutable
public interface PatientParameters {

    @Value.Default
    default String directory() {
        return "/patients";
    }

    String name();

    @Value.Default
    default String referenceGenomeDirectory() {
        return "/reference_genome";
    }

    String referenceGenomeFile();

    default String referenceGenomePath() {
        return referenceGenomeDirectory() + File.separator + referenceGenomeFile();
    }

    @Value.Default
    default String knownIndelDirectory() {
        return "/known_indels";
    }

    List<String> knownIndelFiles();

    default List<String> knownIndelPaths() {
        return knownIndelFiles().stream().map(file -> knownIndelDirectory() + File.separator + file).collect(Collectors.toList());
    }

    @Value.Check
    default void validate() {
        Preconditions.checkState(!knownIndelFiles().isEmpty(), "Pipeline.yaml must contain at least one known indel vcf");
    }
}
