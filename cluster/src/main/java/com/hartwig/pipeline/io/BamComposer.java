package com.hartwig.pipeline.io;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.google.common.collect.Lists;
import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BamComposer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BamComposer.class);
    private final ResultsDirectory resultsDirectory;
    private final int maxComponentsPerCompose;
    private final String optionalSuffix;

    public BamComposer(ResultsDirectory resultsDirectory, int maxComponentsPerCompose, String optionalSuffix) {
        this.resultsDirectory = resultsDirectory;
        this.maxComponentsPerCompose = maxComponentsPerCompose;
        this.optionalSuffix = optionalSuffix;
    }

    BamComposer(ResultsDirectory resultsDirectory, int maxComponentsPerCompose) {
        this(resultsDirectory, maxComponentsPerCompose, "");
    }

    public void run(Sample sample, RuntimeBucket runtimeBucket) {
        LOGGER.info("Composing sharded BAM into a single downloadable BAM file on GS.");
        String headerBlob = runtimeBucket.get(resultsDirectory.path(sample.name() + optionalSuffix() + ".bam_head")).getName();
        String tailDirectory = "%s%s.bam_tail";
        List<Blob> blobs =
                runtimeBucket.list(resultsDirectory.path(String.format(tailDirectory + "/part-r-", sample.name(), optionalSuffix())));
        List<String> toCompose =
                blobs.stream().map(Blob::getName).collect(Collectors.toList());
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
        runtimeBucket.list(directory).forEach(blob -> blob.delete());
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
            runtimeBucket.compose(sources, resultsDirectory.path(sample.name() + optionalSuffix() + ".bam"));
        } else {
            LOGGER.warn("Results directory had no BAM parts to compose. Continuing, but this may mean an error occurred in the pipeline"
                    + "previously");
        }
    }

    private String composePartition(final Sample sample, final RuntimeBucket runtimeBucket, final List<String> toCompose, final int pass,
            final int partitionIndex) {
        String composed = resultsDirectory.path(
                "composed/" + sample.name() + ".bam.part-" + new DecimalFormat("000000").format(partitionIndex) + "-" + pass);
        runtimeBucket.compose(toCompose, composed);
        return runtimeBucket.getNamespace() + "/" + composed;
    }
}
