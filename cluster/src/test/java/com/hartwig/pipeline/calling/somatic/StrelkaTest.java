package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.testsupport.TestConstants;
import com.hartwig.pipeline.tools.Versions;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

public class StrelkaTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new Strelka("reference.bam", "tumor.bam", "strelka.config", "reference_genome.fasta");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.strelka.vcf");
    }


    @Test
    public void runsConfigureStrelkaWorkflow() {
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_DIR + "/strelka/" + Versions.STRELKA + "/bin/configureStrelkaWorkflow.pl "
                + "--tumor tumor.bam --normal reference.bam --config strelka.config --ref reference_genome.fasta "
                + "--output-dir " + outFile("strelkaAnalysis"));
    }

    @Test
    public void runsStrelkaMakefile() {
        assertThat(output.currentBash().asUnixString()).contains("make -C " + outFile("strelkaAnalysis")
                + " -j " + PROC_COUNT + " >>" + TestConstants.LOG_FILE);
    }

    @Test
    public void runsGatkCombineVcf() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar " + TOOLS_GATK_JAR
                + " -T CombineVariants -R reference_genome.fasta --genotypemergeoption unsorted "
                + "-V:snvs " + outFile("strelkaAnalysis/results/passed.somatic.snvs.vcf") + " -V:indels "
                + outFile("strelkaAnalysis/results/passed.somatic.indels.vcf") + " -o " + expectedPath());
    }
}