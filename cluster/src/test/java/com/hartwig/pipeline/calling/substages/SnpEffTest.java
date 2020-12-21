package com.hartwig.pipeline.calling.substages;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class SnpEffTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SnpEff(TestInputs.REG_GENOME_37_RESOURCE_FILES);
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.snpeff.annotated.vcf.gz";
    }

    @Test
    public void runsSnpEff() {
        assertThat(bash()).contains("/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar "
                + "/opt/resources/snpeff/37/snpEff.config GRCh37.75 /data/output/tumor.strelka.vcf /data/output/tumor.snpeff.annotated.vcf");
    }

    @Test
    public void bgzipsOutput() {
        assertThat(bash()).contains("/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.snpeff.annotated.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.snpeff.annotated.vcf.gz -p vcf");
    }
}