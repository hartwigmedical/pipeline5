package com.hartwig.pipeline.tertiary.healthcheck;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class GenerateMetadataCommandTest {

    @Test
    public void createsMetadataInPv4FormateAndAddsToInput() {
        GenerateMetadataCommand victim = new GenerateMetadataCommand(VmDirectories.INPUT, "set", TestInputs.defaultPair());
        assertThat(victim.asBash()).isEqualTo(
                "echo '{\"ref_sample\":\"reference\",\"tumor_sample\":\"tumor\",\"set_name\":\"set\"}' | tee /data/input/metadata");
    }
}