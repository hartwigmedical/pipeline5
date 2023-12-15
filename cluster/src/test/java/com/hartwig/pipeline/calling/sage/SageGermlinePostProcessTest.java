package com.hartwig.pipeline.calling.sage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

import org.junit.Test;

public class SageGermlinePostProcessTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageGermlinePostProcess("tumor");
    }

    @Override
    protected OutputFile input() {
        return OutputFile.of(sampleName(), "sage.germline", "vcf.gz");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.germline.filtered.vcf.gz";
    }

    @Test
    public void testBashCommands() {
        assertThat(output.bash().stream().map(BashCommand::asBash).collect(Collectors.toList())).isEqualTo(expectedCommands());
    }

    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.germline.vcf.gz -O z -o /data/output/tumor.sage.germline.filtered.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.germline.filtered.vcf.gz -p vcf");
    }
}