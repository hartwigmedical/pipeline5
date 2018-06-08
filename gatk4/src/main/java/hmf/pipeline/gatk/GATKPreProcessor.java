package hmf.pipeline.gatk;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.tools.spark.transforms.markduplicates.MarkDuplicatesSpark;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.read.markduplicates.MarkDuplicatesScoringStrategy;
import org.broadinstitute.hellbender.utils.read.markduplicates.OpticalDuplicateFinder;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.patient.Lane;
import hmf.patient.Reference;
import hmf.patient.Sample;
import hmf.pipeline.Stage;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import picard.sam.FastqToSam;

public class GATKPreProcessor implements Stage<Sample> {

    private final ReadsSparkSource readsSparkSource;
    private final JavaSparkContext context;
    private final Reference reference;

    GATKPreProcessor(final Reference reference, final JavaSparkContext context) {
        this.readsSparkSource = new ReadsSparkSource(context);
        this.context = context;
        this.reference = reference;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.DUPLICATE_MARKED;
    }

    @Override
    public void execute(Sample sample) throws IOException {
        if (!sample.lanes().isEmpty()) {
            sample.lanes().forEach(createUBAMFromFastQ(sample.directory(), sample));

            JavaRDD<GATKRead> mergedReads = merged(sample.lanes().stream().map(lane -> {
                String unmappedBamFileName = OutputFile.of(PipelineOutput.UNMAPPED, sample).path();
                return runBwa(unmappedBamFileName, bwaSparkEngine(context, reference, readsSparkSource, unmappedBamFileName));
            }).collect(Collectors.toList()));

            SAMFileHeader firstHeader = readsSparkSource.getHeader(OutputFile.of(PipelineOutput.UNMAPPED, sample).path(), reference.path());
            JavaRDD<GATKRead> alignedDuplicatesMarked = MarkDuplicatesSpark.mark(mergedReads,
                    firstHeader,
                    MarkDuplicatesScoringStrategy.SUM_OF_BASE_QUALITIES,
                    new OpticalDuplicateFinder(),
                    1,
                    false);
            writeOutput(context, sample, firstHeader, alignedDuplicatesMarked);
        }
    }

    private JavaRDD<GATKRead> merged(final List<JavaRDD<GATKRead>> mergedReadsList) {
        JavaRDD<GATKRead> mergedReads = context.emptyRDD();
        for (JavaRDD<GATKRead> gatkReadJavaRDD : mergedReadsList) {
            mergedReads = mergedReads.union(gatkReadJavaRDD);
        }
        return mergedReads;
    }

    private static Consumer<Lane> createUBAMFromFastQ(String directory, Sample sample) {
        return lane -> PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, directory, sample, lane), readFileArgumentOf(2, directory, sample, lane),
                        "SM=" + sample.name(), "O=" + OutputFile.of(PipelineOutput.UNMAPPED, sample).path() }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, String directory, Sample sample, Lane lane) {
        return format("F%s=%s/%s_L00%s_R%s.fastq", sampleIndex, directory, sample.name(), lane.index(), sampleIndex);
    }

    private JavaRDD<GATKRead> runBwa(final String unmappedBamFileName, final BwaSparkEngine bwaEngine) {
        return bwaEngine.align(readsSparkSource.getParallelReads(unmappedBamFileName, reference.path()), true);
    }

    private static BwaSparkEngine bwaSparkEngine(final JavaSparkContext sparkContext, final Reference reference,
            final ReadsSparkSource readsSource, final String unmappedBamFileName) {
        SAMFileHeader header = readsSource.getHeader(unmappedBamFileName, reference.path());
        SAMSequenceDictionary dictionary = dictionary(reference.path(), header);
        return new BwaSparkEngine(sparkContext, reference.path(), null, header, dictionary);
    }

    private static SAMSequenceDictionary dictionary(final String referenceFile, final SAMFileHeader readsHeader) {
        return new ReferenceMultiSource(referenceFile, ReferenceWindowFunctions.IDENTITY_FUNCTION).getReferenceSequenceDictionary(
                readsHeader.getSequenceDictionary());
    }

    private void writeOutput(final JavaSparkContext context, final Sample flowCell, SAMFileHeader header,
            final JavaRDD<GATKRead> alignedReads) throws IOException {
        ReadsSparkSink.writeReads(context, OutputFile.of(output(), flowCell).path(), null, alignedReads, header, ReadsWriteFormat.SINGLE);
    }
}
