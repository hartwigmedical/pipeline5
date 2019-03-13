package com.hartwig.pipeline.io;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
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
    private final String optionalSuffix;

    public BamComposer(Storage storage, ResultsDirectory resultsDirectory, int maxComponentsPerCompose, String optionalSuffix) {
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
        this.maxComponentsPerCompose = maxComponentsPerCompose;
        this.optionalSuffix = optionalSuffix;
    }

    public BamComposer(Storage storage, ResultsDirectory resultsDirectory, int maxComponentsPerCompose) {
        this(storage, resultsDirectory, maxComponentsPerCompose, "");
    }

    public void run(Sample sample, RuntimeBucket runtimeBucket) {
        LOGGER.info("Composing sharded BAM into a single downloadable BAM file on GS.");
        String headerBlob = resultsDirectory.path(sample.name() + optionalSuffix() + ".bam_head");
        String tailDirectory = "%s%s.bam_tail";
        Page<Blob> blobs = storage.list(runtimeBucket.getName(),
                Storage.BlobListOption.prefix(resultsDirectory.path(String.format(tailDirectory + "/part-r-",
                        sample.name(),
                        optionalSuffix()))));
        List<String> toCompose =
                StreamSupport.stream(blobs.iterateAll().spliterator(), false).map(Blob::getName).collect(Collectors.toList());
        if (!toCompose.isEmpty()) {
            toCompose.add(0, headerBlob);
            List<List<String>> partitioned = Lists.partition(toCompose, maxComponentsPerCompose);
            recursivelyCompose(sample, runtimeBucket, partitioned, 1);
            LOGGER.info("Compose complete");
            LOGGER.info("Deleting shards and temporary files");
            deletePath(runtimeBucket, resultsDirectory.path(String.format(tailDirectory, sample.name(), optionalSuffix())));
            deletePath(runtimeBucket, headerBlob);
            deletePath(runtimeBucket, resultsDirectory.path("composed/"));
            LOGGER.info("Delete complete");
        } else {
            LOGGER.info("No BAM parts were found to compose. Skipping this step.");
        }
    }

    private String optionalSuffix() {
        return optionalSuffix.isEmpty() ? "" : "." + optionalSuffix;
    }

    private void deletePath(final RuntimeBucket runtimeBucket, final String directory) {
        storage.list(runtimeBucket.getName(), Storage.BlobListOption.prefix(directory)).iterateAll().forEach(blob -> blob.delete());
    }

    private void recursivelyCompose(final Sample sample, final RuntimeBucket runtimeBucket, final List<List<String>> partitioned,
            int pass) {
        List<String> composed = new ArrayList<>();
        int partitionIndex = 0;
        if (partitioned.size() > 1) {
            for (List<String> partition : partitioned) {
                composed.add(composePartition(sample, runtimeBucket, partition, pass, partitionIndex));
                partitionIndex++;
            }
            recursivelyCompose(sample, runtimeBucket, Lists.partition(composed, maxComponentsPerCompose), pass + 1);
        } else if (partitioned.size() == 1) {
            List<String> sources = partitioned.get(0);
            storage.compose(Storage.ComposeRequest.of(runtimeBucket.getName(),
                    sources,
                    resultsDirectory.path(sample.name() + optionalSuffix() + ".bam")));
        } else {
            LOGGER.warn("Results directory had no BAM parts to compose. Continuing, but this may mean an error occurred in the pipeline"
                    + "previously");
        }
    }

    private String composePartition(final Sample sample, final RuntimeBucket runtimeBucket, final List<String> toCompose, final int pass,
            final int partitionIndex) {
        String composed = resultsDirectory.path(
                "composed/" + sample.name() + ".bam.part-" + new DecimalFormat("000000").format(partitionIndex) + "-" + pass);
        storage.compose(Storage.ComposeRequest.of(runtimeBucket.getName(), toCompose, composed));
        return composed;
    }
}
