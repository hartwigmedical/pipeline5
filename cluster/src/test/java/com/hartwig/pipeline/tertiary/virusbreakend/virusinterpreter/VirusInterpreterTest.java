package com.hartwig.pipeline.tertiary.virusbreakend.virusinterpreter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class VirusInterpreterTest extends TertiaryStageTest<VirusInterpreterOutput> {

    @Override
    protected Stage<VirusInterpreterOutput, SomaticRunMetadata> createVictim() {
        return new VirusInterpreter(TestInputs.virusBreakendOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/virusbreakend/tumor.virusbreakend.vcf.summary.tsv",
                "tumor.virusbreakend.vcf.summary.tsv"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx2G -jar /opt/tools/virus-interpreter/1.0/virus-interpreter.jar -sample_id tumor "
                        + "-virus_breakend_tsv /data/input/tumor.virusbreakend.vcf.summary.tsv "
                        + "-taxonomy_db_tsv /opt/resources/virus_interpreter/taxonomy_db.tsv "
                        + "-virus_interpretation_tsv /opt/resources/virus_interpreter/virus_interpretation.tsv "
                        + "-virus_blacklist_tsv /opt/resources/virus_interpreter/virus_blacklist.tsv "
                        + "-output_dir /data/output");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final VirusInterpreterOutput output) {
        // no additional validation
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final VirusInterpreterOutput output) {
        assertThat(output).isEqualTo(VirusInterpreterOutput.builder().status(PipelineStatus.PERSISTED).build());
    }

    @Override
    protected void validatePersistedOutput(final VirusInterpreterOutput output) {
        assertThat(output).isEqualTo(VirusInterpreterOutput.builder().status(PipelineStatus.PERSISTED).build());
    }

}