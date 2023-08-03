package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.OutputUploadCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Test;

public class OutputUploadCommandTest {
    private static final String OUT_DIR = "/data/output";

    @Test
    public void createsBashToCopyAllFilesAndDirsInOutputFolderToOutputBucket() {
        OutputUploadCommand victim = new OutputUploadCommand(GoogleStorageLocation.of("bucket", "results/"));
        assertThat(victim.asBash()).isEqualTo(format("(gsutil -qm rsync -r %s/ gs://bucket/results/)", OUT_DIR));
    }

    @Test
    public void createsBashToAlsoCopyLogFileToOutputFirst() {
        RuntimeFiles runtimeFiles = RuntimeFiles.of("123");
        OutputUploadCommand victim = new OutputUploadCommand(GoogleStorageLocation.of("bucket", "results/"), runtimeFiles);
        assertThat(victim.asBash()).isEqualTo(format("(cp %s/%s %s && gsutil -qm rsync -r %s/ gs://bucket/results/)",
                BashStartupScript.LOCAL_LOG_DIR,
                runtimeFiles.log(),
                OUT_DIR,
                OUT_DIR));
    }
}