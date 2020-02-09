package com.hartwig.pipeline.calling.command;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Before;
import org.junit.Test;

public class BcfToolsCommandBuilderTest {

    private static final String TABIX = "/opt/tools/tabix/0.2.6/tabix";
    private static final String BCFTOOLS = "/opt/tools/bcftools/1.3.1/bcftools";

    private BcfToolsCommandBuilder victim;

    @Before
    public void setup() {
        victim = new BcfToolsCommandBuilder("input.vcf.gz", "output.vcf.gz");
    }

    @Test
    public void includeHardPass() {
        String bash = victim.includeHardPass().build().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -i 'FILTER=\"PASS\"' input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void pipingMultipleTools() {
        String bash = victim.includeHardFilter("expression1").includeHardFilter("expression2").includeHardFilter("expression3").build().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -i expression1 input.vcf.gz -O u | ");
        assertThat(bash).contains(BCFTOOLS + " -i expression2 -O u | ");
        assertThat(bash).contains(BCFTOOLS + " -i expression3 -O z -o output.vcf.gz");
    }

    @Test
    public void excludeSoftFilter() {
        String bash = victim.excludeSoftFilter("expression", "FLAG").build().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -e expression -s FLAG -m+ input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void removeAnnotation() {
        String bash = victim.removeAnnotation("FLAG").build().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -x FLAG input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void addAnnotation() {
        String bash = victim.addAnnotation("file", "column").build().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -a file -c column input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void addAnnotationWithHeader() {
        String bash = victim.addAnnotation("file", "column", "header").build().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -a file -h header -c column input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void selectSample() {
        String bash = victim.selectSample("sample").build().asBash();
        assertThat(bash).contains(BCFTOOLS + " view -s sample input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test(expected = IllegalStateException.class)
    public void noCommand() {
        victim.build();
    }

    @Test
    public void multipleBuilds() {
        victim.includeHardFilter("expression1");
        String bash1 = victim.build().asBash();
        String bash2 = victim.build().asBash();
        assertThat(bash1).contains(BCFTOOLS + " filter -i expression1 input.vcf.gz -O z -o output.vcf.gz");
        assertThat(bash1).isEqualTo(bash2);

        victim.includeHardFilter("expression2");
        String bash3 = victim.build().asBash();
        assertThat(bash3).contains(BCFTOOLS + " filter -i expression1 input.vcf.gz -O u | /opt/tools/bcftools/1.3.1/bcftools filter -i expression2 -O z -o output.vcf.gz");
    }

    @Test
    public void buildAndIndex() {
        victim.includeHardPass();
        List<BashCommand> commands = victim.buildAndIndex();
        assertThat(commands.get(1).asBash()).contains(TABIX + " output.vcf.gz -p vcf");
    }
}
