package com.hartwig.pipeline.alignment;

import java.util.Optional;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlignmentOutputStorage {

    private final Logger LOGGER = LoggerFactory.getLogger(AlignmentOutputStorage.class);

    private final Storage storage;
    private final Arguments arguments;
    private final ResultsDirectory resultsDirectory;

    public AlignmentOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory) {
        this.storage = storage;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
    }

    public Optional<AlignmentOutput> get(final Sample sample) {
        RuntimeBucket bucket = RuntimeBucket.from(storage, Aligner.NAMESPACE, sample.name(), arguments);
        String sorted = AlignmentOutputPaths.sorted(sample);
        Blob bamBlob = bucket.get(resultsDirectory.path(sorted));
        Blob baiBlob = bucket.get(resultsDirectory.path(AlignmentOutputPaths.bai(sorted)));
        if (bamBlob != null && baiBlob != null) {
            return Optional.of(AlignmentOutput.builder()
                    .status(PipelineStatus.SUCCESS)
                    .maybeFinalBamLocation(location(bucket, bamBlob))
                    .maybeFinalBaiLocation(location(bucket, baiBlob))
                    .sample(sample)
                    .build());
        }
        return Optional.empty();
    }

    @NotNull
    private GoogleStorageLocation location(final RuntimeBucket bucket, final Blob blob) {
        return GoogleStorageLocation.of(bucket.name(), blob.getName().replace(Aligner.NAMESPACE + "/", ""));
    }
}
