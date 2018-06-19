package com.hartwig.pipeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Patient;
import com.hartwig.patient.RawSequencingOutput;
import com.hartwig.patient.Sample;

import org.junit.Test;

public class PipelineTest {

    private static final String NO_DIRECTORY = "";

    @SuppressWarnings("unchecked")
    @Test
    public void stagesSkippedWhenOutputAlreadyExists() throws Exception {
        Sample referenceSample = Sample.builder(NO_DIRECTORY, "reference").build();

        RawSequencingOutput output = mock(RawSequencingOutput.class);
        Stage<Sample, Object> shouldNotBeCalled = mock(Stage.class);
        OutputStore<Sample, Object> duplicatesMarkedExists = mock(OutputStore.class);

        when(shouldNotBeCalled.outputType()).thenReturn(OutputType.DUPLICATE_MARKED);
        when(output.patient()).thenReturn(Patient.of(NO_DIRECTORY,
                NO_DIRECTORY,
                referenceSample,
                Sample.builder(NO_DIRECTORY, "tumour").build()));
        when(duplicatesMarkedExists.exists(referenceSample, OutputType.DUPLICATE_MARKED)).thenReturn(true);
        Pipeline.builder().addPreProcessingStage(shouldNotBeCalled).perSampleStore(duplicatesMarkedExists).build().execute(output);

        verify(shouldNotBeCalled, never()).execute(referenceSample);
    }
}