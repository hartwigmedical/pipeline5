package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;

import org.junit.Test;

public class PonFilterTest extends SubStageTest {

    @Override
    SubStage createVictim() {
        return new PonFilter();
    }

    @Override
    String expectedPath() {
        return "/data/output/tumor.pon.filtered.vcf.gz";
    }

    @Test
    public void runsTwoPipedBcfToolsFilterCommand() {
        assertThat(output.currentBash().asUnixString()).contains("/data/tools/bcftools/1.3.1/bcftools filter -e "
                + "'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5' -s GERMLINE_PON -m+ /data/output/tumor.strelka.vcf -O u | "
                + "/data/tools/bcftools/1.3.1/bcftools filter -e 'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3' -s SOMATIC_PON "
                + "-m+  -O z -o /data/output/tumor.pon.filtered.vcf.gz >>/data/output/run.log 2>&1 || die\n");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains(
                "/data/tools/tabix/0.2.6/tabix /data/output/tumor.pon.filtered.vcf.gz -p vcf");
    }
}