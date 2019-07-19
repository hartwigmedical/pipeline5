package com.hartwig.pipeline.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineMain;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.tools.Versions;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(value = IntegrationTest.class)
public class SmokeTest {

    @Test
    public void runFullPipelineAndAssertNoErrors() {
        PipelineMain victim = new PipelineMain();
        PipelineState state = victim.start(Arguments.defaultsBuilder(Arguments.DefaultsProfile.DEVELOPMENT.toString())
                .privateKeyPath("google-key.json")
                .sampleDirectory(System.getProperty("user.dir") + "/samples")
                .jarDirectory(System.getProperty("user.dir") + "system/target")
                .cloudSdkPath("/usr/bin")
                .nodeInitializationScript(System.getProperty("user.dir") + "cluster/src/main/resources/node-init.sh")
                .setId("CPCT12345678")
                .mode(Arguments.Mode.FULL)
                .runId("smoke-test-" + version())
                .runGermlineCaller(false)
                .cleanup(false)
                .build());
        assertThat(state.status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    private String version() {
        String version = Versions.class.getPackage().getImplementationVersion();
        return version != null ? version : System.getProperty("user.name");
    }
}
