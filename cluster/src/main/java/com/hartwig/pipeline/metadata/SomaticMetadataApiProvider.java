package com.hartwig.pipeline.metadata;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.api.HmfApi;
import com.hartwig.api.model.Run;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pubsub.EventPublisher;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.transfer.staged.SetResolver;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.jetbrains.annotations.NotNull;

public class SomaticMetadataApiProvider {

    private final Arguments arguments;
    private final Storage storage;
    private final EventPublisher<PipelineComplete> publisher;

    private SomaticMetadataApiProvider(final Arguments arguments, final Storage storage, final EventPublisher<PipelineComplete> publisher) {
        this.arguments = arguments;
        this.storage = storage;
        this.publisher = publisher;
    }

    public static SomaticMetadataApiProvider from(final Arguments arguments, final Storage storage,
            final EventPublisher<PipelineComplete> publisher) {
        return new SomaticMetadataApiProvider(arguments, storage, publisher);
    }

    public SomaticMetadataApi get() {
        return arguments.sbpApiRunId()
                .map(this::diagnosticRun)
                .orElseGet(() -> arguments.biopsy().map(this::researchRun).orElseGet(localRun()));
    }

    @NotNull
    public Supplier<SomaticMetadataApi> localRun() {
        return () -> new LocalSomaticMetadata(arguments,
                arguments.sampleJson()
                        .map(JsonSampleSource::new)
                        .orElseThrow(() -> new IllegalArgumentException("Sample JSON must be provided when running in local mode")),
                createPublisher(SetResolver.forLocal(), Optional.of(new Run().id(0L)), Pipeline.Context.PLATINUM, false));
    }

    public SomaticMetadataApi researchRun(final String biopsyName) {
        HmfApi api = HmfApi.create(arguments.sbpApiUrl());
        Optional<Run> run = arguments.sbpApiRunId().map(id -> api.runs().get((long) id));
        return new ResearchMetadataApi(api.samples(),
                api.sets(),
                api.runs(),
                run,
                biopsyName,
                arguments,
                createPublisher(SetResolver.forApi(api.sets()),
                        run.or(() -> Optional.of(new Run().id(0L))),
                        Pipeline.Context.RESEARCH,
                        true),
                new Anonymizer(arguments));
    }

    public SomaticMetadataApi diagnosticRun(final Integer setId) {
        HmfApi api = HmfApi.create(arguments.sbpApiUrl());
        Run run = api.runs().get((long) arguments.sbpApiRunId().orElseThrow());
        return new DiagnosticSomaticMetadataApi(run,
                api.runs(),
                api.samples(),
                createPublisher(SetResolver.forApi(api.sets()), Optional.of(run), arguments.context(), false),
                new Anonymizer(arguments));
    }

    private StagedOutputPublisher createPublisher(final SetResolver setResolver, final Optional<Run> run, final Pipeline.Context context,
            final boolean useOnlyDBSets) {
        Bucket sourceBucket = storage.get(arguments.outputBucket());
        return new StagedOutputPublisher(setResolver, sourceBucket, publisher, run, context, arguments.outputCram(), useOnlyDBSets);
    }
}
