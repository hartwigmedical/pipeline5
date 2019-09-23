package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_BCFTOOLS;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class CosmicAnnotationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new CosmicAnnotation("cosmic.vcf.gz", "ID,INFO");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.cosmic.annotated.vcf.gz");
    }

    @Test
    public void runsBcfToolsDbSnpAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains(
                TOOLS_BCFTOOLS + " annotate -a cosmic.vcf.gz -c ID,INFO "
                        + "-o " + expectedPath() + " -O z " + outFile("tumor.strelka.vcf"));
    }
}