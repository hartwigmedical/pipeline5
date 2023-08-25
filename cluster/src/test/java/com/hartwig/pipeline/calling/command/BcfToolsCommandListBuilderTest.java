package com.hartwig.pipeline.calling.command;

import com.hartwig.computeengine.execution.vm.command.BashCommand;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BcfToolsCommandListBuilderTest {

    private static final String TABIX = "/opt/tools/tabix/0.2.6/tabix";
    private static final String BCFTOOLS = "/opt/tools/bcftools/1.9/bcftools";

    private BcfToolsCommandListBuilder victim;

    @Before
    public void setup() {
        victim = new BcfToolsCommandListBuilder("input.vcf.gz", "output.vcf.gz");
    }

    @Test
    public void includeHardPass() {
        String bash = victim.includeHardPass().bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -i 'FILTER=\"PASS\"' input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void testExpressionQuote() {
        String bash = victim.includeHardFilter("'expression1").includeHardFilter("expression2'").includeHardFilter("'expression3'").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -i 'expression1' input.vcf.gz -O u | ");
        assertThat(bash).contains(BCFTOOLS + " filter -i 'expression2' -O u | ");
        assertThat(bash).contains(BCFTOOLS + " filter -i 'expression3' -O z -o output.vcf.gz");
    }

    @Test
    public void pipingMultipleTools() {
        String bash = victim.includeHardFilter("expression1").includeHardFilter("expression2").includeHardFilter("expression3").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -i 'expression1' input.vcf.gz -O u | ");
        assertThat(bash).contains(BCFTOOLS + " filter -i 'expression2' -O u | ");
        assertThat(bash).contains(BCFTOOLS + " filter -i 'expression3' -O z -o output.vcf.gz");
    }

    @Test
    public void excludeSoftFilter() {
        String bash = victim.excludeSoftFilter("expression", "FLAG").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " filter -e 'expression' -s FLAG -m+ input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void removeAnnotation() {
        String bash = victim.removeAnnotation("FLAG").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -x FLAG input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void addAnnotation() {
        String bash = victim.addAnnotation("file", "column").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -a file -c column input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void addAnnotationWithFlagButNoAnnotations() {
        String bash = victim.addAnnotationWithFlag("file", "flag").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -a file -m flag input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void addAnnotationWithFlag() {
        String bash = victim.addAnnotationWithFlag("file", "flag", "column1", "column2").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -a file -m flag -c column1,column2 input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void addAnnotationWithHeader() {
        String bash = victim.addAnnotationWithHeader("file", "column", "header").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " annotate -a file -h header -c column input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void selectSample() {
        String bash = victim.selectSample("sample").bcfCommand().asBash();
        assertThat(bash).contains(BCFTOOLS + " view -s sample input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test(expected = IllegalStateException.class)
    public void noCommand() {
        victim.bcfCommand();
    }

    @Test
    public void multipleBuilds() {
        victim.includeHardFilter("expression1");
        String bash1 = victim.bcfCommand().asBash();
        String bash2 = victim.bcfCommand().asBash();
        assertThat(bash1).contains(BCFTOOLS + " filter -i 'expression1' input.vcf.gz -O z -o output.vcf.gz");
        assertThat(bash1).isEqualTo(bash2);

        victim.includeHardFilter("expression2");
        String bash3 = victim.bcfCommand().asBash();
        assertThat(bash3).contains(BCFTOOLS + " filter -i 'expression1' input.vcf.gz -O u | /opt/tools/bcftools/1.9/bcftools filter -i 'expression2' -O z -o output.vcf.gz");
    }

    @Test
    public void withIndex() {
        victim.withIndex().includeHardPass();
        List<BashCommand> commands = victim.build();
        assertThat(commands.get(1).asBash()).contains(TABIX + " output.vcf.gz -p vcf");
    }
}
