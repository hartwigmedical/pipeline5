package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;

import org.junit.Test;

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
                "/opt/tools/bcftools/1.3.1/bcftools annotate -a dbsnp.vcf.gz -c ID -o " + expectedPath() + " -O z " + outFile(
                        "tumor.strelka.vcf"));
    }
}