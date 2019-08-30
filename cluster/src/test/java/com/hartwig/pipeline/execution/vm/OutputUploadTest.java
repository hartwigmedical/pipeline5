package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Test;

public class OutputUploadTest {

    @Test
    public void createsBaseToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo("gsutil -qm -o GSUtil:parallel_composite_upload_threshold=150M cp -r /data/output/ gs://bucket/results/");
    }
}