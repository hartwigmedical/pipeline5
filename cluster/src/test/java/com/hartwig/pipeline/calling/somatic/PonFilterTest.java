package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Test;

public class PonFilterTest extends SubStageTest implements CommonEntities {

    @Override
    public SubStage createVictim() {
        return new PonFilter();
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.pon.filtered.vcf.gz";
    }

    @Test
    public void runsTwoPipedBcfToolsFilterCommandInSubshell() {
        assertThat(bash()).contains("(/opt/tools/bcftools/1.3.1/bcftools filter -e "
                + "'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5' -s GERMLINE_PON -m+ /data/output/tumor.strelka.vcf -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools filter -e 'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3' -s SOMATIC_PON "
                + "-m+  -O z -o /data/output/tumor.pon.filtered.vcf.gz)");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.pon.filtered.vcf.gz -p vcf");
    }
}