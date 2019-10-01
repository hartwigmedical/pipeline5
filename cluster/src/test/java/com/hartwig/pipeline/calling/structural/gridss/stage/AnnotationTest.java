package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@Ignore
public class AnnotationTest extends SubStageTest implements CommonEntities {

    private static final String UNTEMPLATED_OUTPUT_VCF_NAME = format("%s/%s.annotation.vcf", OUT_DIR, JOINT_NAME);
    private String rawVcf;
    private String generatedAnnotatedOutputVcfName;
    private String assemblyBam;
    private ArgumentCaptor<BashCommand> captor;

    @Override
    public SubStage createVictim() {
        return new Annotation(REFERENCE_BAM, TUMOR_BAM, assemblyBam, REFERENCE_GENOME, JOINT_NAME, CONFIG_FILE, BLACKLIST);
    }

    @Override
    public String expectedPath() {
        return UNTEMPLATED_OUTPUT_VCF_NAME + ".gz";
    }

    @Override
    public String sampleName() {
        return JOINT_NAME;
    }

    @Before
    public void setup() {
        rawVcf = "raw.vcf";
        final OutputFile input = mock(OutputFile.class);
        final OutputFile output = mock(OutputFile.class);
        when(input.path()).thenReturn(rawVcf);
        when(output.path()).thenReturn(expectedPath());
        final BashStartupScript initialScript = mock(BashStartupScript.class);
        captor = ArgumentCaptor.forClass(BashCommand.class);

        assemblyBam = "assembly.bam";
        generatedAnnotatedOutputVcfName = format("%s/%s.annotated_variants.vcf", OUT_DIR, JOINT_NAME);

    }

    @Test
    public void shouldAddCommandsInCorrectOrder() {
        List<BashCommand> commands = captor.getAllValues();
        assertThat(commands.size()).isEqualTo(4);
        assertThat(commands.get(0).getClass().isAssignableFrom(AnnotateVariants.class)).isEqualTo(true);
        assertThat(commands.get(1).getClass().isAssignableFrom(AnnotateUntemplatedSequence.class)).isEqualTo(true);
        assertThat(commands.get(2).getClass().isAssignableFrom(BgzipCommand.class)).isEqualTo(true);
        assertThat(commands.get(3).getClass().isAssignableFrom(TabixCommand.class)).isEqualTo(true);
    }

    @Test
    public void shouldCreateCorrectAnnotateVariants() {
        assertThat(captor.getAllValues().get(0).asBash()).isEqualTo(new AnnotateVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam,
                rawVcf, REFERENCE_GENOME, generatedAnnotatedOutputVcfName, CONFIG_FILE, BLACKLIST).asBash());
    }

    @Test
    public void shouldCreateCorrectAnnotateUntemplatedSequence() {
        assertThat(captor.getAllValues().get(1).asBash()).isEqualTo((new AnnotateUntemplatedSequence(generatedAnnotatedOutputVcfName, REFERENCE_GENOME,
                UNTEMPLATED_OUTPUT_VCF_NAME).asBash()));
    }

    @Test
    public void shouldCreateCorrectBgzip() {
        assertThat(captor.getAllValues().get(2).asBash()).isEqualTo(new BgzipCommand(UNTEMPLATED_OUTPUT_VCF_NAME).asBash());
    }

    @Test
    public void shouldCreateCorrectTabix() {
        assertThat(captor.getAllValues().get(3).asBash()).isEqualTo(new TabixCommand(expectedPath()).asBash());
    }
}