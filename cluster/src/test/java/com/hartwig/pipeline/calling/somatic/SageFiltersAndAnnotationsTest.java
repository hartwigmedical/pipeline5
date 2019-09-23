package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_BCFTOOLS;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_BCFTOOLS + " filter -i 'FILTER=\"PASS\"' "
                + outFile("tumor.strelka.vcf") + " -O u | " + TOOLS_BCFTOOLS + " annotate -x INFO/HOTSPOT -O u | "
                + TOOLS_BCFTOOLS + " annotate -x FILTER/LOW_CONFIDENCE -O u | "
                + TOOLS_BCFTOOLS + " annotate -x FILTER/GERMLINE_INDEL -O u | "
                + TOOLS_BCFTOOLS + " view -s tumor -O z -o " + outFile("tumor.sage.hotspots.filtered.vcf.gz"));
    }
}