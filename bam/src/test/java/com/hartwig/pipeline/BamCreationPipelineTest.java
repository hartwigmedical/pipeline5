package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class BamCreationPipelineTest {

    private static final ImmutableSample SAMPLE = Sample.builder("", "TEST").build();
    private static final InputOutput<AlignmentRecordDataset> ALIGNED_BAM = InputOutput.of(SAMPLE, mock(AlignmentRecordDataset.class));
    private static final InputOutput<AlignmentRecordDataset> ENRICHED_BAM = InputOutput.of(SAMPLE, mock(AlignmentRecordDataset.class));
    private static final InputOutput<AlignmentRecordDataset> FINAL_BAM = InputOutput.of(SAMPLE, mock(AlignmentRecordDataset.class));
    private InputOutput<AlignmentRecordDataset> lastStored;
    private InputOutput<AlignmentRecordDataset> lastStoredWithSuffix;
    private String lastSuffix;
    private StatusReporter.Status lastStatus;

    @Test
    public void storesEnrichedBamWhenAllQCsPassAndNoErrors() {
        BamCreationPipeline victim = createPipeline(QCResult.ok());
        victim.execute(SAMPLE);
        assertThat(lastStatus).isEqualTo(StatusReporter.Status.SUCCESS);
        assertThat(lastStored).isEqualTo(ENRICHED_BAM);
    }

    @Test
    public void storesFinalQCFailures() {
        BamCreationPipeline victim = createPipeline(QCResult.failure("final"));
        victim.execute(SAMPLE);
        assertThat(lastStatus).isEqualTo(StatusReporter.Status.FAILED_FINAL_QC);
    }

    @NotNull
    private ImmutableBamCreationPipeline createPipeline(QCResult finalQC) {
        return BamCreationPipeline.builder()
                .alignment(input -> ALIGNED_BAM)
                .markDuplicates(markDups())
                .finalBamStore(finalStore())
                .finalDatasource(sample -> FINAL_BAM)
                .finalQC(toQC -> finalQC)
                .statusReporter(status -> lastStatus = status)
                 .build();
    }

    @NotNull
    private OutputStore<AlignmentRecordDataset> finalStore() {
        return new OutputStore<AlignmentRecordDataset>() {
            @Override
            public void store(final InputOutput<AlignmentRecordDataset> inputOutput) {
                lastStored = inputOutput;
            }

            @Override
            public void store(final InputOutput<AlignmentRecordDataset> inputOutput, final String suffix) {
                lastStoredWithSuffix = inputOutput;
                lastSuffix = suffix;
            }

            @Override
            public boolean exists(final Sample sample) {
                return false;
            }

            @Override
            public void clear() {
                // do nothin
            }
        };
    }

    @NotNull
    private Stage<AlignmentRecordDataset, AlignmentRecordDataset> markDups() {
        return input -> ENRICHED_BAM;
    }
}