package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.storage.GoogleStorageLocation;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class OutputUploadTest {

    @Test
    public void createsBaseToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo(format("(cp %s %s && %s)",
                LOG_FILE, OUT_DIR, copyOutputToStorage("gs://bucket/results/")));
    }
}