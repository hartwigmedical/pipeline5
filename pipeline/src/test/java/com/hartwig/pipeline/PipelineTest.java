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
import com.hartwig.patient.Sample;

import org.junit.Before;
import org.junit.Test;

public class PipelineTest {

    private static final String NO_DIRECTORY = "";
    private Stage<Sample, Object, Object> shouldNotBeCalled;
    private OutputStore<Sample, Object> duplicatesMarkedExists;
    private DataSource<Sample, Object> returnsObjectFromFileSystem;
    private InputOutput<Sample, Object> priorInput;
    private Patient patient;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        final Sample referenceSample = Sample.builder(NO_DIRECTORY, "reference").build();

        shouldNotBeCalled = mock(Stage.class);
        duplicatesMarkedExists = mock(OutputStore.class);
        returnsObjectFromFileSystem = mock(DataSource.class);

        priorInput = InputOutput.of(OutputType.ALIGNED, referenceSample, new Object());

        when(returnsObjectFromFileSystem.extract(referenceSample)).thenReturn(priorInput);
        when(shouldNotBeCalled.outputType()).thenReturn(OutputType.DUPLICATE_MARKED);
        patient = Patient.of(NO_DIRECTORY, NO_DIRECTORY, referenceSample, Sample.builder(NO_DIRECTORY, "maybeTumour").build());
        when(duplicatesMarkedExists.exists(referenceSample, OutputType.DUPLICATE_MARKED)).thenReturn(true);
    }

    @Test
    public void stagesSkippedWhenOutputAlreadyExists() throws Exception {
        Pipeline.builder().addPreProcessingStage(shouldNotBeCalled).bamStore(duplicatesMarkedExists).build().execute(patient);
        verify(shouldNotBeCalled, never()).execute(priorInput);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void priorOutputRetrievedFromFileSystemWhenStageSkipped() throws Exception {

        Stage<Sample, Object, Object> shouldBeCalled = mock(Stage.class);
        when(shouldBeCalled.datasource()).thenReturn(returnsObjectFromFileSystem);
        Pipeline.builder()
                .addPreProcessingStage(shouldNotBeCalled).addPreProcessingStage(shouldBeCalled).bamStore(duplicatesMarkedExists)
                .build().execute(patient);

        verify(shouldBeCalled, times(1)).execute(priorInput);
    }
}