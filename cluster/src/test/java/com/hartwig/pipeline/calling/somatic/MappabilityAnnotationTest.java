package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_BCFTOOLS;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class MappabilityAnnotationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new MappabilityAnnotation("mappability.bed", "mappability.hdr");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.mappability.annotated.vcf.gz");
    }

    @Test
    public void runsBcfToolsMappabilityAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_BCFTOOLS + " annotate -a "
                + "mappability.bed -h mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY -o " + expectedPath()
                + " -O z " + outFile("tumor.strelka.vcf"));
    }
}