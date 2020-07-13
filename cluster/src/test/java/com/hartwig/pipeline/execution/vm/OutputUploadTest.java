package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Test;

public class OutputUploadTest {
    private static final String OUT_DIR = "/data/output";

    @Test
    public void createsBashToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo(format("(gsutil -qm rsync -r %s/ gs://bucket/results/)", OUT_DIR));
    }

    @Test
    public void createsBashToAlsoCopyLogFileToOutputFirst() {
        RuntimeFiles runtimeFiles = RuntimeFiles.of("123");
        OutputUpload victim = new OutputUpload(GoogleStorageLocation.of("bucket", "results/"), runtimeFiles);
        assertThat(victim.asBash()).isEqualTo(format("(cp %s/%s %s && gsutil -qm rsync -r %s/ gs://bucket/results/)",
                BashStartupScript.LOCAL_LOG_DIR,
                runtimeFiles.log(),
                OUT_DIR,
                OUT_DIR));
    }
}