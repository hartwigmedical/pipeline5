package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;

import org.junit.Before;
import org.junit.Test;

public class AnnotationTest implements CommonEntities {

    private String sampleBam;
    private String tumorBam;
    private String rawVcf;
    private String annotatedVcf;
    private String annotatedUntemplatedVcf;

    private CommandFactory factory;
    private GridssToBashCommandConverter converter;

    private AnnotateVariants annotateVariants;
    private AnnotateUntemplatedSequence annotateUntemplated;
    private BgzipCommand bgzip;
    private TabixCommand tabix;
    private Annotation.AnnotationResult result;
    private String assemblyBam;
    private JavaClassCommand annotateVariantsBash;
    private JavaClassCommand annotateUntemplatedBash;
    private String annotateVariantsBashCommands;
    private String annotateUntemplatedBashCommands;

    @Before
    public void setup() {
        sampleBam = "sample.bam";
        tumorBam = "tumor.bam";
        rawVcf = "raw.vcf";
        assemblyBam = "assembly.bam";

        annotatedVcf = "annotated.vcf";
        annotatedUntemplatedVcf = "annotated_untemplated.vcf";

        factory = mock(CommandFactory.class);
        converter = mock(GridssToBashCommandConverter.class);

        annotateVariants = mock(AnnotateVariants.class);
        when(annotateVariants.resultantVcf()).thenReturn(annotatedVcf);
        when(factory.buildAnnotateVariants(any(), any(), any(), any(), any(), any())).thenReturn(annotateVariants);
        annotateVariantsBash = mock(JavaClassCommand.class);
        when(converter.convert(annotateVariants)).thenReturn(annotateVariantsBash);
        annotateVariantsBashCommands = "annotate variants bash";
        when(annotateVariantsBash.asBash()).thenReturn(annotateVariantsBashCommands);

        annotateUntemplated = mock(AnnotateUntemplatedSequence.class);
        when(annotateUntemplated.resultantVcf()).thenReturn(annotatedUntemplatedVcf);
        when(factory.buildAnnotateUntemplatedSequence(any(), any(), any())).thenReturn(annotateUntemplated);
        annotateUntemplatedBash = mock(JavaClassCommand.class);
        when(converter.convert(annotateUntemplated)).thenReturn(annotateUntemplatedBash);
        annotateUntemplatedBashCommands = "annotate untemplated bash";
        when(annotateUntemplatedBash.asBash()).thenReturn(annotateUntemplatedBashCommands);

        bgzip = mock(BgzipCommand.class);
        when(factory.buildBgzipCommand(any())).thenReturn(bgzip);

        tabix = mock(TabixCommand.class);
        when(factory.buildTabixCommand(any())).thenReturn(tabix);

        result = new Annotation(factory, converter).initialise(sampleBam, tumorBam, assemblyBam, rawVcf, REFERENCE_GENOME, JOINT_NAME);
    }

    @Test
    public void shouldReturnPathToFinishedVcf() {
        assertThat(result.annotatedVcf()).isEqualTo(format("%s.gz", annotatedUntemplatedVcf));
    }

    @Test
    public void shouldReturnBashCommandOfAllCommandsConcatenatedTogether() {
        List<BashCommand> generatedCommands = result.commands();
        assertThat(generatedCommands).isNotEmpty();
        assertThat(generatedCommands.size()).isEqualTo(4);
        assertThat(generatedCommands.get(0).asBash()).isEqualTo(annotateVariantsBashCommands);
        assertThat(generatedCommands.get(1).asBash()).isEqualTo(annotateUntemplatedBashCommands);
        assertThat(generatedCommands.get(2)).isEqualTo(bgzip);
        assertThat(generatedCommands.get(3)).isEqualTo(tabix);
    }
}