package com.hartwig.pipeline.execution.vm;

import com.hartwig.support.test.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.hartwig.pipeline.execution.vm.BashStartupScript.of;
import static org.assertj.core.api.Assertions.assertThat;

public class BashStartupScriptTest {
    private BashStartupScript scriptBuilder;
    private String bucketName;

    @Before
    public void setup() {
        bucketName = "outputBucket";
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
    public void shouldWriteCompleteScript() throws IOException {
        String expectedScript = Resources.testResource("script_generation/complete_script");
        String simpleCommand = "uname -a";
        String complexCommand = "not_really_so_complex";
        scriptBuilder.addLine(simpleCommand);
        scriptBuilder.addCommand(() -> complexCommand);
        assertThat(scriptBuilder.asUnixString()).isEqualTo(new String(Files.readAllBytes(Paths.get(expectedScript))));
    }
}