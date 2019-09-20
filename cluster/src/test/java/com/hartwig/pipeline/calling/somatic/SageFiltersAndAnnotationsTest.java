package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;

import org.junit.Test;

public class SageFiltersAndAnnotationsTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageFiltersAndAnnotations("tumor");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.sage.hotspots.filtered.vcf.gz");
    }

    @Test
    public void pipesBcfToolsFilterAndAnnotations() {
        assertThat(output.currentBash().asUnixString()).contains("/opt/tools/bcftools/1.3.1/bcftools filter -i 'FILTER=\"PASS\"' "
                + outFile("tumor.strelka.vcf") + " -O u | /opt/tools/bcftools/1.3.1/bcftools annotate -x INFO/HOTSPOT -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools annotate -x FILTER/LOW_CONFIDENCE -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools annotate -x FILTER/GERMLINE_INDEL -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools view -s tumor -O z -o " + outFile("tumor.sage.hotspots.filtered.vcf.gz"));
    }
}