package com.hartwig.pipeline.io;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BamComposer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamComposer.class);
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;
    private final int maxComponentsPerCompose;

    public BamComposer(final Storage storage, final ResultsDirectory resultsDirectory, final int maxComponentsPerCompose) {
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
        this.maxComponentsPerCompose = maxComponentsPerCompose;
    }

    public void run(Sample sample, RuntimeBucket runtimeBucket) {
        LOGGER.info("Composing sharded BAM into a single downloadable BAM file on GS.");
        String headerBlob = resultsDirectory.path(sample.name() + ".bam_head");
        String tailDirectory = "%s.bam_tail";
        List<String> toCompose = StreamSupport.stream(storage.list(runtimeBucket.getName(),
                Storage.BlobListOption.prefix(resultsDirectory.path(String.format(tailDirectory + "/part-r-", sample.name()))))
                .iterateAll()
                .spliterator(), false).map(Blob::getName).collect(Collectors.toList());
        toCompose.add(0, headerBlob);
        List<List<String>> partitioned = Lists.partition(toCompose, maxComponentsPerCompose);
        recursivelyCompose(sample, runtimeBucket, partitioned);
        LOGGER.info("Compose complete");
        LOGGER.info("Deleting shards and temporary files");
        deletePath(runtimeBucket, resultsDirectory.path(String.format(tailDirectory, sample.name())));
        deletePath(runtimeBucket, headerBlob);
        deletePath(runtimeBucket, resultsDirectory.path("composed/"));
        LOGGER.info("Delete complete");
    }

    private void deletePath(final RuntimeBucket runtimeBucket, final String directory) {
        storage.list(runtimeBucket.getName(), Storage.BlobListOption.prefix(directory)).iterateAll().forEach(blob -> blob.delete());
    }

    private void recursivelyCompose(final Sample sample, final RuntimeBucket runtimeBucket, final List<List<String>> partitioned) {
        List<String> composed = new ArrayList<>();
        int partitionIndex = 0;
        if (partitioned.size() > 1) {
            for (List<String> partition : partitioned) {
                composed.add(composePartition(sample, runtimeBucket, partition, partitionIndex));
                partitionIndex++;
            }
            recursivelyCompose(sample, runtimeBucket, Lists.partition(composed, maxComponentsPerCompose));
        } else if (partitioned.size() == 1) {
            storage.compose(Storage.ComposeRequest.of(runtimeBucket.getName(),
                    partitioned.get(0),
                    resultsDirectory.path(sample.name() + ".bam")));
        } else {
            LOGGER.warn("Results directory had no BAM parts to compose. Continuing, but this may mean an error occurred in the pipeline"
                    + "previously");
        }
    }

    private String composePartition(final Sample sample, final RuntimeBucket runtimeBucket, final List<String> toCompose,
            final int partitionIndex) {
        String composed =
                resultsDirectory.path("composed/" + sample.name() + ".bam.part-" + new DecimalFormat("000000").format(partitionIndex));
        storage.compose(Storage.ComposeRequest.of(runtimeBucket.getName(), toCompose, composed));
        return composed;
    }
}
