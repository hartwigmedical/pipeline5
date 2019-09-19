package com.hartwig.pipeline.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
@Ignore
public class SmokeTest {

    @Test
    public void runFullPipelineAndCheckFinalStatus() {
        PipelineMain victim = new PipelineMain();
        String version = System.getProperty("version");
        PipelineState state = victim.start(Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath("google-key.json")
                .sampleDirectory(workingDir() + "/../samples")
                .version(version)
                .jarDirectory(workingDir() + "/../bam/target")
                .cloudSdkPath("/usr/bin")
                .nodeInitializationScript(workingDir() + "/src/main/resources/node-init.sh")
                .setId("CPCT12345678")
                .mode(Arguments.Mode.FULL)
                .runId("smoke-" + noDots(version))
                .runGermlineCaller(false)
                .build());
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);
    }

    private static String workingDir() {
        return System.getProperty("user.dir");
    }

    private static String noDots(final String version) {
        return version.replace(".", "");
    }
}