package com.hartwig.pipeline.metadata;

import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.api.HmfApi;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.jetbrains.annotations.NotNull;

public class SomaticMetadataApiProvider {

    private final Arguments arguments;
    private final Storage storage;
    private final Publisher publisher;

    private SomaticMetadataApiProvider(final Arguments arguments, final Storage storage, final Publisher publisher) {
        this.arguments = arguments;
        this.storage = storage;
        this.publisher = publisher;
    }

    public static SomaticMetadataApiProvider from(final Arguments arguments, final Storage storage, final Publisher publisher) {
        return new SomaticMetadataApiProvider(arguments, storage, publisher);
    }

    public SomaticMetadataApi get() {
        return arguments.sbpApiRunId()
                .map(this::productionStyleRun)
                .orElseGet(() -> arguments.biopsy().map(this::biopsyBasedRerun).orElseGet(localRun()));
    }

    @NotNull
    public Supplier<SomaticMetadataApi> localRun() {
        return () -> new LocalSomaticMetadata(arguments,
                arguments.sampleJson()
                        .map(JsonSampleSource::new)
                        .orElseThrow(() -> new IllegalArgumentException("Sample JSON must be provided when running in local mode")));
    }

    public SomaticMetadataApi biopsyBasedRerun(final String biopsyName) {
        HmfApi api = HmfApi.create(arguments.sbpApiUrl());
        Bucket sourceBucket = storage.get(arguments.outputBucket());
        ObjectMapper objectMapper = ObjectMappers.get();
        return new BiopsyMetadataApi(api.samples(),
                api.sets(),
                biopsyName,
                arguments,
                new StagedOutputPublisher(api.sets(), sourceBucket, publisher, objectMapper, null));
    }

    public SomaticMetadataApi productionStyleRun(final Integer setId) {
        HmfApi api = HmfApi.create(arguments.sbpApiUrl());
        Bucket sourceBucket = storage.get(arguments.outputBucket());
        ObjectMapper objectMapper = ObjectMappers.get();
        return new ClinicalSomaticMetadataApi(arguments,
                setId,
                SbpRestApi.newInstance(arguments.sbpApiUrl()),
                new StagedOutputPublisher(api.sets(),
                        sourceBucket,
                        publisher,
                        objectMapper,
                        api.runs().get((long) arguments.sbpApiRunId().orElseThrow())));
    }
}
