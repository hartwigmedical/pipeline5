package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SageFiltersAndAnnotationsTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageFiltersAndAnnotations("tumor");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.hotspots.filtered.vcf.gz";
    }

    @Test
    public void pipesBcfToolsFilterAndAnnotations() {
        assertThat(output.currentBash().asUnixString()).contains("/opt/tools/bcftools/1.3.1/bcftools filter -i 'FILTER=\"PASS\"' "
                + "/data/output/tumor.strelka.vcf -O u | /opt/tools/bcftools/1.3.1/bcftools annotate -x INFO/HOTSPOT -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools annotate -x FILTER/LOW_CONFIDENCE -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools annotate -x FILTER/GERMLINE_INDEL -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools view -s tumor -O z -o /data/output/tumor.sage.hotspots.filtered.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.filtered.vcf.gz -p vcf");
    }
}