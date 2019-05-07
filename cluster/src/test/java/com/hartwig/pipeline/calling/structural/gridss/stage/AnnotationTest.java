package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import com.hartwig.pipeline.calling.structural.gridss.process.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.process.AnnotateVariants;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class AnnotationTest {
    private String sampleBam;
    private String tumorBam;
    private String rawVcf;
    private String annotatedVcf;
    private String annotatedUntemplatedVcf;

    private CommandFactory factory;
    private AnnotateVariants annotateVariants;
    private AnnotateUntemplatedSequence annotateUntemplated;
    private BgzipCommand bgzip;
    private TabixCommand tabix;
    private Annotation.AnnotationResult result;

    @Before
    public void setup() {
        sampleBam = "sample.bam";
        tumorBam = "tumor.bam";
        rawVcf = "raw.vcf";

        annotatedVcf = "annotated.vcf";
        annotatedUntemplatedVcf = "annotated_untemplated.vcf";

        factory = mock(CommandFactory.class);

        annotateVariants = mock(AnnotateVariants.class);
        when(annotateVariants.resultantVcf()).thenReturn(annotatedVcf);
        when(factory.buildAnnotateVariants(any(), any(), any())).thenReturn(annotateVariants);

        annotateUntemplated = mock(AnnotateUntemplatedSequence.class);
        when(annotateUntemplated.resultantVcf()).thenReturn(annotatedUntemplatedVcf);
        when(factory.buildAnnotateUntemplatedSequence(any(), any())).thenReturn(annotateUntemplated);

        bgzip = mock(BgzipCommand.class);
        when(factory.buildBgzipCommand(any())).thenReturn(bgzip);

        tabix = mock(TabixCommand.class);
        when(factory.buildTabixCommand(any())).thenReturn(tabix);

        result = new Annotation(factory).initialise(sampleBam, tumorBam, rawVcf, TestConstants.REF_GENOME);
    }

    @Test
    public void shouldRequestBuildOfAnnotateVariantsPassingReferenceBamAndTumorBamAndRawVcf() {
        verify(factory).buildAnnotateVariants(sampleBam, tumorBam, rawVcf);
    }

    @Test
    public void shouldRequestBuildOfAnnotateUntemplatedSequenceUsingResultOfPreviousCommand() {
        verify(factory).buildAnnotateUntemplatedSequence(annotatedVcf, TestConstants.REF_GENOME);
    }

    @Test
    public void shouldRequestBgzipOfAnnotatedSequence() {
        verify(factory).buildBgzipCommand(annotatedUntemplatedVcf);
    }

    @Test
    public void shouldRequestTabixOfBgzippedSequence() {
        verify(factory).buildTabixCommand(format("%s.gz", annotatedUntemplatedVcf));
    }

    @Test
    public void shouldReturnResult() {
        assertThat(result).isNotNull();
    }

    @Test
    public void shouldReturnPathToFinishedVcf() {
        assertThat(result.annotatedVcf()).isEqualTo(format("%s.gz", annotatedUntemplatedVcf));
    }

    @Test
    public void shouldReturnBashCommandOfAllCommandsConcatenatedTogether() {
        assertThat(result.commands()).isEqualTo(asList(annotateVariants, annotateUntemplated, bgzip, tabix));
    }
}