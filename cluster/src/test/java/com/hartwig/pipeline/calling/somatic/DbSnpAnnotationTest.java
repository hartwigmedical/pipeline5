package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class DbSnpAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new DbSnpAnnotation("dbsnp.vcf.gz");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.dbsnp.annotated.vcf.gz";
    }

    @Test
    public void runsBcfToolsDbSnpAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("/data/tools/bcftools/1.3.1/bcftools annotate -a dbsnp.vcf.gz -c ID -o "
                + "/data/output/tumor.dbsnp.annotated.vcf.gz -O z /data/output/tumor.strelka.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains("/data/tools/tabix/0.2.6/tabix /data/output/tumor.dbsnp.annotated.vcf.gz "
                + "-p vcf >>/data/output/run.log");
    }
}