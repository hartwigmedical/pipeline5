package com.hartwig.pipeline.alignment.merge;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.StorageProvider;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MergeAndMarkDupsTest {

    @Test
    public void runMergeMarkDups() throws Exception {
        Arguments development = Arguments.defaultsBuilder("development").runId("mergemarkdups").build();
        GoogleCredentials credentials = CredentialProvider.from(development).get();
        ComputeEngine computeEngine = ComputeEngine.from(development, credentials);
        Storage storage = StorageProvider.from(development, credentials).get();
        MergeAndMarkDups mergeAndMarkDups = new MergeAndMarkDups(computeEngine, ResultsDirectory.defaultDirectory(), storage, development);

        PerLaneAlignmentOutput perLaneAlignmentOutput = PerLaneAlignmentOutput.builder()
                .addBams(ImmutableIndexedBamLocation.builder()
                        .bam(GoogleStorageLocation.of("run-cpct12345678r-mergemarkdups/aligner", "L001/results/001-aligned-sorted.bam"))
                        .bai(GoogleStorageLocation.of("run-cpct12345678r-mergemarkdups/aligner", "L001/results/001-aligned-sorted.bam.bai"))
                        .build())
                .addBams(ImmutableIndexedBamLocation.builder()
                        .bam(GoogleStorageLocation.of("run-cpct12345678r-mergemarkdups/aligner", "L002/results/002-aligned-sorted.bam"))
                        .bai(GoogleStorageLocation.of("run-cpct12345678r-mergemarkdups/aligner", "L002/results/002-aligned-sorted.bam.bai"))
                        .build())
                .build();

        mergeAndMarkDups.run(SingleSampleRunMetadata.builder()
                .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                .sampleId("CPCT12345678R")
                .build(), perLaneAlignmentOutput);

    }

}