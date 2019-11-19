package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import org.junit.Test;

public class DbSnpAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new DbSnpAnnotation("dbsnp.vcf.gz");
    }

    @Override
    public String expectedPath() {
        return VmDirectories.outputFile("tumor.dbsnp.annotated.vcf.gz");
    }

    @Test
    public void runsBcfToolsDbSnpAnnotation() {
        assertThat(bash()).contains("/opt/tools/bcftools/1.3.1/bcftools annotate -a dbsnp.vcf.gz -c ID -o " + expectedPath()
                + " -O z /data/output/tumor.strelka.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains("/opt/tools/tabix/0.2.6/tabix " + expectedPath());
    }
}