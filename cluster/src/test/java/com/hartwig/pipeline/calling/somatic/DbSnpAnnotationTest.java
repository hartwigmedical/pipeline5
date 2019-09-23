package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_BCFTOOLS;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class DbSnpAnnotationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new DbSnpAnnotation("dbsnp.vcf.gz");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.dbsnp.annotated.vcf.gz");
    }

    @Test
    public void runsBcfToolsDbSnpAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains(
                TOOLS_BCFTOOLS + " annotate -a dbsnp.vcf.gz -c ID -o " + expectedPath() + " -O z "
                        + outFile("tumor.strelka.vcf"));
    }
}