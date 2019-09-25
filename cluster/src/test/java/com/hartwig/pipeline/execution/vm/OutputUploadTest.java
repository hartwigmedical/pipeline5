package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.TestConstants.LOG_FILE;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.BucketInputOutput;

import org.junit.Test;

public class OutputUploadTest {

    @Test
    public void createsBaseToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo(format("(cp %s %s && %s)",
                LOG_FILE,
                OUT_DIR,
                new BucketInputOutput("bucket").output("results/")));
    }
}