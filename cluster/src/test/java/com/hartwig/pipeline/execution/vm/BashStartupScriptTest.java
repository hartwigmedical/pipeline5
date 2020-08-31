package com.hartwig.pipeline.execution.vm;

import static com.hartwig.pipeline.execution.vm.BashStartupScript.of;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import com.hartwig.pipeline.execution.vm.storage.LocalSsdStorageStrategy;
import com.hartwig.pipeline.testsupport.Resources;

import org.junit.Before;
import org.junit.Test;

public class BashStartupScriptTest {
    private BashStartupScript scriptBuilder;
    private String bucketName;

    @Before
    public void setup() {
        bucketName = "outputBucket";
        scriptBuilder = of(bucketName);
    }

    @Test
    public void shouldWriteCompleteScriptForLocalSsds() throws IOException {
        String expectedScript = Resources.testResource("script_generation/complete_script-local_ssds");
        String simpleCommand = "uname -a";
        scriptBuilder.addLine(simpleCommand);
        scriptBuilder.addCommand(new ComplexCommand());
        LocalSsdStorageStrategy storageStrategy = new LocalSsdStorageStrategy(4);
        assertThat(scriptBuilder.asUnixString(storageStrategy, Optional.empty())).isEqualTo(new String(Files.readAllBytes(Paths.get(
                expectedScript))));
    }

    private static class ComplexCommand implements BashCommand {
        @Override
        public String asBash() {
            return "not_really_so_complex \"quoted\"";
        }
    }
}