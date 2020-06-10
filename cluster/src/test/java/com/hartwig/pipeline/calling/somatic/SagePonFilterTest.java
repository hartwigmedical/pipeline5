package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SagePonFilterTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SagePonFilter();
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.pon.filter.vcf.gz";
    }

    @Test
    public void runsTwoPipedBcfToolsFilterCommandInSubshell() {
        assertThat(bash()).contains("/opt/tools/bcftools/1.9/bcftools filter -e "
                + "'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0' -s SAGE_PON -m+ /data/output/tumor.strelka.vcf "
                + "-O z -o /data/output/tumor.sage.pon.filter.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf");
    }
}