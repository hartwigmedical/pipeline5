package com.hartwig.pipeline.resource;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;

import org.immutables.value.Value;

@Value.Immutable
public interface Resources {

    Resource referenceGenome();

    Resource knownIndels();

    Resource knownSnps();

    static Resources from(Storage storage, Arguments arguments) {
        Resource referenceGenome = new Resource(storage, arguments.referenceGenomeBucket(), "reference_genome", new ReferenceGenomeAlias());
        Resource knownIndels = new Resource(storage, arguments.knownIndelsBucket(), "known_indels");
        Resource knownSnps = new Resource(storage, "known_snps", "known_snps");
        return ImmutableResources.builder()
                .referenceGenome(referenceGenome)
                .knownIndels(knownIndels)
                .knownSnps(knownSnps)
                .build();
    }
}
