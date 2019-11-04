package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Test;

public class OutputUploadTest implements CommonEntities {

    @Test
    public void createsBaseToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results"));
        assertThat(victim.asBash()).isEqualTo("(cp /var/log/run.log /data/output && gsutil rm -r gs://bucket/results && gsutil -qm cp -r "
                + "/data/output/ gs://bucket/results)");
    }
}