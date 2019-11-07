package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Test;

public class OutputUploadTest implements CommonEntities {

    @Test
    public void createsBaseToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo(format("(cp %s %s && gsutil -qm rsync -r %s/ gs://bucket/results/)",
                LOG_FILE, OUT_DIR, OUT_DIR));
    }
}