package com.hartwig.pipeline;

import static java.lang.String.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline<BAM, VCF> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);
    private final List<Stage<Sample, BAM, BAM>> preProcessors;
    private final Stage<Sample, BAM, VCF> germlineCalling;
    private final OutputStore<Sample, BAM> bamStore;
    private final OutputStore<Sample, VCF> vcfStore;

    private Pipeline(final List<Stage<Sample, BAM, BAM>> preProcessors, final Stage<Sample, BAM, VCF> germlineCalling,
            final OutputStore<Sample, BAM> bamStore, final OutputStore<Sample, VCF> vcfStore) {
        this.preProcessors = preProcessors;
        this.germlineCalling = germlineCalling;
        this.bamStore = bamStore;
        this.vcfStore = vcfStore;
    }

    public void execute(Patient patient) throws IOException {
        LOGGER.info("Preprocessing started for reference sample");
        LOGGER.info("Storing results in {}", OutputFile.RESULTS_DIRECTORY);
        long startTime = startTimer();
        for (Stage<Sample, BAM, BAM> preProcessor : preProcessors) {
            if (!bamStore.exists(patient.reference(), preProcessor.outputType())) {
                runStage(patient.reference(), preProcessor, bamStore);
            } else {
                LOGGER.info("Skipping [{}] stage as the output already exists in [{}]",
                        preProcessor.outputType(),
                        OutputFile.RESULTS_DIRECTORY);
            }
        }
        if (germlineCalling != null) {
            LOGGER.info("Experimental germline calling is enabled");
            runStage(patient.reference(), germlineCalling, vcfStore);
        }
        LOGGER.info("Preprocessing complete for reference sample, Took {} ms", (endTimer() - startTime));
    }

    private <I, O> void runStage(final Sample sample, final Stage<Sample, I, O> stage, final OutputStore<Sample, O> store)
            throws IOException {
        InputOutput<Sample, I> input = stage.datasource().extract(sample);
        Trace trace = Trace.of(Pipeline.class, format("Executing [%s] stage", stage.getClass().getSimpleName())).start();
        store.store(stage.execute(input == null ? InputOutput.seed(sample) : input));
        trace.finish();
    }

    private static long endTimer() {
        return System.currentTimeMillis();
    }

    private static long startTimer() {
        return System.currentTimeMillis();
    }

    public static <BAM, VCF> Pipeline.Builder<BAM, VCF> builder() {
        return new Builder<>();
    }

    public static class Builder<BAM, VCF> {
        private final List<Stage<Sample, BAM, BAM>> preProcessors = new ArrayList<>();
        private Stage<Sample, BAM, VCF> germlineCalling;
        private OutputStore<Sample, BAM> bamStore;
        private OutputStore<Sample, VCF> vcfStore;

        public Builder<BAM, VCF> addPreProcessingStage(Stage<Sample, BAM, BAM> preProcessor) {
            this.preProcessors.add(preProcessor);
            return this;
        }

        public Builder<BAM, VCF> germlineCalling(final Stage<Sample, BAM, VCF> germlineCalling) {
            this.germlineCalling = germlineCalling;
            return this;
        }

        public Builder<BAM, VCF> bamStore(OutputStore<Sample, BAM> bamStore) {
            this.bamStore = bamStore;
            return this;
        }

        public Builder<BAM, VCF> vcfStore(OutputStore<Sample, VCF> vcfStore) {
            this.vcfStore = vcfStore;
            return this;
        }

        public Pipeline<BAM, VCF> build() {
            return new Pipeline<>(preProcessors, germlineCalling, bamStore, vcfStore);
        }
    }
}
