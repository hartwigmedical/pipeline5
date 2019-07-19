package com.hartwig.pipeline.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
public class SmokeTest {

    @Test
    public void runFullPipelineAndAssertNoErrors() {
        PipelineMain victim = new PipelineMain();
        String version = System.getProperty("version");
        PipelineState state = victim.start(Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath("google-key.json")
                .sampleDirectory(System.getProperty("user.dir") + "/../samples")
                .version(version)
                .jarDirectory(System.getProperty("user.dir") + "/../system/target")
                .cloudSdkPath("/usr/bin")
                .nodeInitializationScript(System.getProperty("user.dir") + "/src/main/resources/node-init.sh")
                .setId("CPCT12345678")
                .mode(Arguments.Mode.FULL)
                .runId("smoke-" + version)
                .runGermlineCaller(false)
                .cleanup(false)
                .build());
        assertThat(state.status()).isEqualTo(PipelineStatus.SUCCESS);
    }
}
