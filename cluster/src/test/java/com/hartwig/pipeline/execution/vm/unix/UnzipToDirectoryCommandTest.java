package com.hartwig.pipeline.execution.vm.unix;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.unix.UnzipToDirectoryCommand;

import org.junit.Test;

public class UnzipToDirectoryCommandTest {
    @Test
    public void shouldGenerateCommand() {
        final String outDir = "/the/output/directory";
        final String zipFile = "some_zip_file.zip";
        assertThat(new UnzipToDirectoryCommand(outDir, zipFile).asBash()).isEqualTo("unzip -d " + outDir + " " + zipFile);
    }
}
