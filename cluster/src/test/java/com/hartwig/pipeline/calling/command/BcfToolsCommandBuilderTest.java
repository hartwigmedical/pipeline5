package com.hartwig.pipeline.calling.command;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Before;
import org.junit.Test;

public class BcfToolsCommandBuilderTest {

    private BcfToolsCommandBuilder victim;

    @Before
    public void setup() {
        victim = new BcfToolsCommandBuilder("input.vcf.gz", "output.vcf.gz");
    }

    @Test
    public void testIncludeHardPass() {
        String bash = victim.includeHardPass().build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools filter -i 'FILTER=\"PASS\"' input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void testPiping() {
        String bash = victim.includeHardFilter("expression1").includeHardFilter("expression2").includeHardFilter("expression3").build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools filter -i expression1 input.vcf.gz -O u | ");
        assertThat(bash).contains("| /opt/tools/bcftools/1.3.1/bcftools filter -i expression2 -O u | ");
        assertThat(bash).contains("| /opt/tools/bcftools/1.3.1/bcftools filter -i expression3 -O z -o output.vcf.gz");
    }

    @Test
    public void testExcludeSoftFilter() {
        String bash = victim.excludeSoftFilter("expression", "FLAG").build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools filter -e expression -s FLAG -m+ input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void testRemoveAnnotation() {
        String bash = victim.removeAnnotation("FLAG").build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools annotate -x FLAG input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void testAddAnnotation() {
        String bash = victim.addAnnotation("file", "column").build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools annotate -a file -c column input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void testAddAnnotationWithHeader() {
        String bash = victim.addAnnotation("file", "column", "header").build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools annotate -a file -h header -c column input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test
    public void testSelectSample() {
        String bash = victim.selectSample("sample").build().asBash();
        assertThat(bash).contains("/opt/tools/bcftools/1.3.1/bcftools view -s sample input.vcf.gz -O z -o output.vcf.gz");
    }

    @Test(expected = IllegalStateException.class)
    public void testNoCommand() {
        victim.build();
    }

    @Test
    public void testMultipleBuilds() {
        victim.includeHardFilter("expression1");
        String bash1 = victim.build().asBash();
        String bash2 = victim.build().asBash();
        assertThat(bash1).contains("/opt/tools/bcftools/1.3.1/bcftools filter -i expression1 input.vcf.gz -O z -o output.vcf.gz");
        assertThat(bash1).isEqualTo(bash2);

        victim.includeHardFilter("expression2");
        String bash3 = victim.build().asBash();
        assertThat(bash3).contains("/opt/tools/bcftools/1.3.1/bcftools filter -i expression1 input.vcf.gz -O u | /opt/tools/bcftools/1.3.1/bcftools filter -i expression2 -O z -o output.vcf.gz");
    }

    @Test
    public void testIndex() {
        victim.includeHardPass();
        List<BashCommand> commands = victim.buildAndIndex();
        assertThat(commands.get(1).asBash()).contains("/opt/tools/tabix/0.2.6/tabix output.vcf.gz -p vcf");
    }
}
