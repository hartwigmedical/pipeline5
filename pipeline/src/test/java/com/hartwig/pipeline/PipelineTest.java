package com.hartwig.pipeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Patient;
import com.hartwig.patient.RawSequencingOutput;
import com.hartwig.patient.Sample;

import org.junit.Before;
import org.junit.Test;

public class PipelineTest {

    private static final String NO_DIRECTORY = "";
    private RawSequencingOutput output;
    private Stage<Sample, Object> shouldNotBeCalled;
    private OutputStore<Sample, Object> duplicatesMarkedExists;
    private DataSource<Sample, Object> returnsObjectFromFileSystem;
    private InputOutput<Sample, Object> priorInput;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        final Sample referenceSample = Sample.builder(NO_DIRECTORY, "reference").build();

        output = mock(RawSequencingOutput.class);
        shouldNotBeCalled = mock(Stage.class);
        duplicatesMarkedExists = mock(OutputStore.class);
        returnsObjectFromFileSystem = mock(DataSource.class);

        priorInput = InputOutput.of(OutputType.ALIGNED, referenceSample, new Object());

        when(returnsObjectFromFileSystem.extract(referenceSample)).thenReturn(priorInput);
        when(shouldNotBeCalled.outputType()).thenReturn(OutputType.DUPLICATE_MARKED);
        when(output.patient()).thenReturn(Patient.of(NO_DIRECTORY, NO_DIRECTORY, referenceSample, Sample.builder(NO_DIRECTORY, "tumour").build()));
        when(duplicatesMarkedExists.exists(referenceSample, OutputType.DUPLICATE_MARKED)).thenReturn(true);
    }

    @Test
    public void stagesSkippedWhenOutputAlreadyExists() throws Exception {
        Pipeline.builder().addPreProcessingStage(shouldNotBeCalled).perSampleStore(duplicatesMarkedExists).build().execute(output);
        verify(shouldNotBeCalled, never()).execute(priorInput);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void priorOutputRetrievedFromFileSystemWhenStageSkipped() throws Exception {

        Stage<Sample, Object> shouldBeCalled = mock(Stage.class);
        when(shouldBeCalled.datasource()).thenReturn(returnsObjectFromFileSystem);
        Pipeline.builder()
                .addPreProcessingStage(shouldNotBeCalled)
                .addPreProcessingStage(shouldBeCalled)
                .perSampleStore(duplicatesMarkedExists)
                .build()
                .execute(output);

        verify(shouldBeCalled, times(1)).execute(priorInput);
    }
}