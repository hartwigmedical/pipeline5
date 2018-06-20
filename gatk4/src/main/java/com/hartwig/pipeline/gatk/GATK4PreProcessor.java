package com.hartwig.pipeline.gatk;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.hartwig.exception.Exceptions;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Lane;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.tools.spark.transforms.markduplicates.MarkDuplicatesSpark;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.markduplicates.MarkDuplicatesScoringStrategy;
import org.broadinstitute.hellbender.utils.read.markduplicates.OpticalDuplicateFinder;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import picard.sam.FastqToSam;

public class GATK4PreProcessor implements Stage<Sample, ReadsAndHeader> {

    private final ReadsSparkSource readsSparkSource;
    private final JavaSparkContext context;
    private final ReferenceGenome referenceGenome;

    GATK4PreProcessor(final ReferenceGenome referenceGenome, final JavaSparkContext context) {
        this.readsSparkSource = new ReadsSparkSource(context);
        this.context = context;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }

    @Override
    public InputOutput<Sample, ReadsAndHeader> execute(InputOutput<Sample, ReadsAndHeader> input) throws IOException {
        Sample sample = input.entity();
        if (!sample.lanes().isEmpty()) {
            sample.lanes().forEach(createUBAMFromFastQ(sample));

            JavaRDD<GATKRead> mergedReads = merged(sample.lanes().stream().map(lane -> {
                String unmappedBamFileName = OutputFile.of(OutputType.UNMAPPED, sample).path();
                return runBwa(unmappedBamFileName, bwaSparkEngine(context, referenceGenome, readsSparkSource, unmappedBamFileName));
            }).collect(Collectors.toList()));

            SAMFileHeader firstHeader =
                    readsSparkSource.getHeader(OutputFile.of(OutputType.UNMAPPED, sample).path(), referenceGenome.path());
            JavaRDD<GATKRead> alignedDuplicatesMarked = MarkDuplicatesSpark.mark(mergedReads,
                    firstHeader,
                    MarkDuplicatesScoringStrategy.SUM_OF_BASE_QUALITIES,
                    new OpticalDuplicateFinder(),
                    1,
                    false);
            return InputOutput.of(outputType(), sample, ReadsAndHeader.of(alignedDuplicatesMarked, firstHeader));
        }
        throw Exceptions.noLanesInSample();
    }

    private JavaRDD<GATKRead> merged(final List<JavaRDD<GATKRead>> mergedReadsList) {
        JavaRDD<GATKRead> mergedReads = context.emptyRDD();
        for (JavaRDD<GATKRead> gatkReadJavaRDD : mergedReadsList) {
            mergedReads = mergedReads.union(gatkReadJavaRDD);
        }
        return mergedReads;
    }

    private static Consumer<Lane> createUBAMFromFastQ(Sample sample) {
        return lane -> PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, lane.readsFile()), readFileArgumentOf(2, lane.matesFile()), "SM=" + sample.name(),
                        "O=" + OutputFile.of(OutputType.UNMAPPED, sample).path() }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, String pathname) {
        return format("F%s=%s", sampleIndex, pathname);
    }

    private JavaRDD<GATKRead> runBwa(final String unmappedBamFileName, final BwaSparkEngine bwaEngine) {
        return bwaEngine.align(readsSparkSource.getParallelReads(unmappedBamFileName, referenceGenome.path()), true);
    }

    private static BwaSparkEngine bwaSparkEngine(final JavaSparkContext sparkContext, final ReferenceGenome referenceGenome,
            final ReadsSparkSource readsSource, final String unmappedBamFileName) {
        SAMFileHeader header = readsSource.getHeader(unmappedBamFileName, referenceGenome.path());
        SAMSequenceDictionary dictionary = dictionary(referenceGenome.path(), header);
        return new BwaSparkEngine(sparkContext, referenceGenome.path(), null, header, dictionary);
    }

    private static SAMSequenceDictionary dictionary(final String referenceFile, final SAMFileHeader readsHeader) {
        return new ReferenceMultiSource(referenceFile, ReferenceWindowFunctions.IDENTITY_FUNCTION).getReferenceSequenceDictionary(
                readsHeader.getSequenceDictionary());
    }
}
