package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.vm.storage.LocalSsdStorageStrategy;
import com.hartwig.support.test.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static com.hartwig.pipeline.execution.vm.BashStartupScript.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BashStartupScriptTest {
    private BashStartupScript scriptBuilder;

    @Before
    public void setup() {
        String bucketName = "outputBucket";
        scriptBuilder = of(bucketName);
    }

    @Test
    public void shouldReturnSuccessFlagFilename() {
        assertThat(scriptBuilder.successFlag()).isEqualTo("JOB_SUCCESS");
    }

    @Test
    public void shouldReturnFailureFlagFilename() {
        assertThat(scriptBuilder.failureFlag()).isEqualTo("JOB_FAILURE");
    }

    @Test
    public void shouldWriteCompleteScriptInLocalSsdMode() throws IOException {
        String expectedScript = Resources.testResource("script_generation/local-ssd_script");
        String simpleCommand = "uname -a";
        scriptBuilder.addLine(simpleCommand);
        scriptBuilder.addCommand(new ComplexCommand());
        LocalSsdStorageStrategy storage = mock(LocalSsdStorageStrategy.class);
        when(storage.initialise()).thenReturn(Collections.singletonList("echo \"raid device creation commands\""));
        assertThat(scriptBuilder.asUnixString(storage)).isEqualTo(new String(Files.readAllBytes(Paths.get(expectedScript))));
    }

    @Test
    public void shouldWriteCompleteScriptInPersistentStorageMode() throws IOException {
        String expectedScript = Resources.testResource("script_generation/persistent-storage_script");
        String simpleCommand = "uname -a";
        scriptBuilder.addLine(simpleCommand);
        scriptBuilder.addCommand(new ComplexCommand());
        assertThat(scriptBuilder.asUnixString()).isEqualTo(new String(Files.readAllBytes(Paths.get(expectedScript))));
    }

    private class ComplexCommand implements BashCommand {
        @Override
        public String asBash() {
            return "not_really_so_complex \"quoted\"";
        }
    }
}