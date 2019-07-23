package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AnnotationTest extends GridssSubStageTest {

    private String rawVcf;
    private String generatedAnnotatedOutputVcfName;
    private String generatedUntemplatedOutputVcfName;
    private String assemblyBam;
    private ArgumentCaptor<List<BashCommand>> commandsList;

    @Override
    public SubStage createVictim() {
        return new Annotation(REFERENCE_BAM, TUMOR_BAM, assemblyBam, rawVcf, REFERENCE_GENOME, JOINT_NAME, CONFIG_FILE, BLACKLIST);
    }

    @Override
    public String expectedPath() {
        return format("%s/%s.annotation.vcf.gz", OUT_DIR, JOINT_NAME);
    }

    @Override
    public String sampleName() {
        return JOINT_NAME;
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        final OutputFile input = mock(OutputFile.class);
        final OutputFile output = mock(OutputFile.class);
        when(input.path()).thenReturn("raw.vcf");
        final BashStartupScript initialScript = mock(BashStartupScript.class);
        captor = ArgumentCaptor.forClass(BashCommand.class);

        assemblyBam = "assembly.bam";
        rawVcf = "raw.vcf";
        generatedAnnotatedOutputVcfName = format("%s/%s.annotated_variants.vcf", OUT_DIR, JOINT_NAME);
        generatedUntemplatedOutputVcfName = format("%s/%s_untemplated.vcf", OUT_DIR, JOINT_NAME);

        commandsList = ArgumentCaptor.forClass(List.class);
        final BashStartupScript finishedScript = createVictim().bash(input, output, initialScript);
        verify(finishedScript).addCommands(commandsList.capture());
    }

    @Test
    public void shouldAddCommandsInCorrectOrder() {
        List<BashCommand> commands = commandsList.getValue();
        assertThat(commands.size()).isEqualTo(4);
        assertThat(commands.get(0).getClass().isAssignableFrom(AnnotateVariants.class)).isEqualTo(true);
        assertThat(commands.get(1).getClass().isAssignableFrom(AnnotateUntemplatedSequence.class)).isEqualTo(true);
        assertThat(commands.get(2).getClass().isAssignableFrom(BgzipCommand.class)).isEqualTo(true);
        assertThat(commands.get(3).getClass().isAssignableFrom(TabixCommand.class)).isEqualTo(true);
    }

    @Test
    public void shouldCreateCorrectAnnotateVariants() {
        List<BashCommand> commands = commandsList.getValue();
        assertThat(commands.get(0).asBash()).isEqualTo(new AnnotateVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam,
                rawVcf, REFERENCE_GENOME, generatedAnnotatedOutputVcfName, CONFIG_FILE, BLACKLIST).asBash());
    }

    @Test
    public void shouldCreateCorrectAnnotateUntemplatedSequence() {
        List<BashCommand> commands = commandsList.getValue();
        assertThat(commands.get(1).asBash()).isEqualTo((new AnnotateUntemplatedSequence(generatedAnnotatedOutputVcfName, REFERENCE_GENOME,
                generatedUntemplatedOutputVcfName).asBash()));
    }

    @Test
    public void shouldCreateCorrectBgzip() {
        List<BashCommand> commands = commandsList.getValue();
        assertThat(commands.get(2).asBash()).isEqualTo(new BgzipCommand(generatedUntemplatedOutputVcfName).asBash());
    }

    @Test
    public void shouldCreateCorrectTabix() {
        List<BashCommand> commands = commandsList.getValue();
        assertThat(commands.get(3).asBash()).isEqualTo(new TabixCommand(generatedUntemplatedOutputVcfName + ".gz").asBash());
    }
}