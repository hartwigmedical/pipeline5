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
        Resource referenceGenome =
                new Resource(storage, arguments.referenceGenomeBucket(), arguments.referenceGenomeBucket(), new ReferenceGenomeAlias());
        Resource knownIndels = new Resource(storage, arguments.knownIndelsBucket(), arguments.knownIndelsBucket());
        Resource knownSnps = new Resource(storage, "known_snps", "known_snps");
        return ImmutableResources.builder().referenceGenome(referenceGenome).knownIndels(knownIndels).knownSnps(knownSnps).build();
    }
}
