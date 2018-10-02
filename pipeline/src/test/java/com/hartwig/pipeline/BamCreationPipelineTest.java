package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.MoreExecutors;
import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class BamCreationPipelineTest {

    private static final ImmutableSample SAMPLE = Sample.builder("", "TEST").build();
    private static final InputOutput<AlignmentRecordRDD> ALIGNED_BAM =
            InputOutput.of(OutputType.ALIGNED, SAMPLE, mock(AlignmentRecordRDD.class));
    private static final Patient PATIENT = Patient.of("", "TEST", SAMPLE, SAMPLE);
    private InputOutput<AlignmentRecordRDD> lastStored;
    private static final InputOutput<AlignmentRecordRDD> ENRICHED =
            InputOutput.of(OutputType.DUPLICATE_MARKED, SAMPLE, mock(AlignmentRecordRDD.class));

    @Before
    public void setUp() throws Exception {
        lastStored = null;
    }

    @Test
    public void alignsAndStoresBAM() throws Exception {
        builder().bamStore(bamStore(false, false)).build().execute(PATIENT);
        assertThat(lastStored).isEqualTo(ALIGNED_BAM);
    }

    private ImmutableBamCreationPipeline.Builder builder() {
        return BamCreationPipeline.builder()
                .executorService(MoreExecutors.newDirectExecutorService())
                .alignment(alignmentStage()).alignmentDatasource(sample -> ALIGNED_BAM).finalDatasource(sample -> ALIGNED_BAM)
                .readCountQCFactory(aligned -> reads -> QCResult.ok())
                .finalBamStore(bamStore(false, false)).indexBam(mock(IndexBam.class))
                .referenceFinalQC(reads -> QCResult.ok())
                .tumorFinalQC(reads -> QCResult.ok())
                .monitor(metric -> {
                });
    }

    @Test
    public void enrichesAlignedBAM() throws Exception {
        builder().addBamEnrichment(enrichmentStage(ENRICHED)).bamStore(bamStore(false, false)).build().execute(PATIENT);
        assertThat(lastStored).isEqualTo(ENRICHED);
    }

    @Test
    public void skipsAlignmentWhenOutputAlreadyExists() throws Exception {
        builder().bamStore(bamStore(false, true)).build().execute(PATIENT);
        assertThat(lastStored).isNull();
    }

    @Test
    public void skipsEnrichmentWhenOutputAlreadyExists() throws Exception {
        builder().addBamEnrichment(enrichmentStage(ENRICHED)).bamStore(bamStore(true, false)).build().execute(PATIENT);
        assertThat(lastStored).isEqualTo(ALIGNED_BAM);
    }

    @Test(expected = ExecutionException.class)
    public void executionExceptionWhenReadCountQCFails() throws Exception {
        builder().bamStore(bamStore(false, false))
                .addBamEnrichment(enrichmentStage(ENRICHED)).readCountQCFactory(aligned -> reads -> QCResult.failure("test"))
                .build()
                .execute(PATIENT);
    }

    @Test(expected = ExecutionException.class)
    public void executionExceptionWhenReferenceFinalQCFails() throws Exception {
        builder().bamStore(bamStore(false, false))
                .addBamEnrichment(enrichmentStage(ENRICHED))
                .referenceFinalQC(reads -> QCResult.failure("test"))
                .build()
                .execute(PATIENT);
    }

    @Test(expected = ExecutionException.class)
    public void executionExceptionWhenTumorFinalQCFails() throws Exception {
        builder().bamStore(bamStore(false, false))
                .addBamEnrichment(enrichmentStage(ENRICHED)).tumorFinalQC(reads -> QCResult.failure("test"))
                .build()
                .execute(PATIENT);
    }

    @NotNull
    private OutputStore<AlignmentRecordRDD> bamStore(final boolean enrichedExists, final boolean alignedExists) {
        return new OutputStore<AlignmentRecordRDD>() {
            @Override
            public void store(final InputOutput<AlignmentRecordRDD> inputOutput) {
                lastStored = inputOutput;
            }

            @Override
            public boolean exists(final Sample sample, final OutputType type) {
                if (alignedExists) {
                    if (type == OutputType.ALIGNED) {
                        return true;
                    }
                }
                if (enrichedExists) {
                    if (type != OutputType.ALIGNED) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    @NotNull
    private static Stage<AlignmentRecordRDD, AlignmentRecordRDD> enrichmentStage(final InputOutput<AlignmentRecordRDD> enriched) {
        return new Stage<AlignmentRecordRDD, AlignmentRecordRDD>() {
            @Override
            public DataSource<AlignmentRecordRDD> datasource() {
                return sample -> ALIGNED_BAM;
            }

            @Override
            public OutputType outputType() {
                return OutputType.DUPLICATE_MARKED;
            }

            @Override
            public InputOutput<AlignmentRecordRDD> execute(final InputOutput<AlignmentRecordRDD> input) throws IOException {
                return enriched;
            }
        };
    }

    @NotNull
    private static AlignmentStage alignmentStage() {
        return input -> ALIGNED_BAM;
    }
}