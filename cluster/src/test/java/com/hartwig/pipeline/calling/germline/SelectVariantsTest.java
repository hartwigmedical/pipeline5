package com.hartwig.pipeline.calling.germline;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_GATK_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class SelectVariantsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new SelectVariants("snp", Lists.newArrayList("SNP,NO_VARIATION"), "referenceSampleName.fasta");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.raw_snp.vcf");
    }

    @Test
    public void selectsVariantsWithGatk() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar " + TOOLS_GATK_JAR
                + " -T SelectVariants -selectType SNP,NO_VARIATION -R referenceSampleName.fasta -V "
                + outFile("tumor.strelka.vcf") + " -o " + expectedPath());
    }
}