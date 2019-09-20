package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.CommonTestEntities;

import org.junit.Test;

public class OutputUploadTest implements CommonTestEntities {

    @Test
    public void createsBaseToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo(format("(cp %s %s && %s)",
                LOG_FILE, OUT_DIR, copyOutputToStorage("gs://bucket/results/")));
    }
}