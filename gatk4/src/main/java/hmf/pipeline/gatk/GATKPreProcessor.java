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
import hmf.pipeline.Stage;
import hmf.sample.FlowCell;
import hmf.sample.Lane;
import hmf.sample.Reference;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import picard.sam.FastqToSam;

public class GATKPreProcessor implements Stage<FlowCell> {

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
    public void execute(FlowCell flowCell) throws IOException {
        if (!flowCell.lanes().isEmpty()) {
            flowCell.lanes().forEach(createUBAMFromFastQ());

            JavaRDD<GATKRead> mergedReads = merged(flowCell.lanes().stream().map(lane -> {
                String unmappedBamFileName = OutputFile.of(PipelineOutput.UNMAPPED, lane).path();
                return runBwa(unmappedBamFileName, bwaSparkEngine(context, reference, readsSparkSource, unmappedBamFileName));
            }).collect(Collectors.toList()));

            SAMFileHeader firstHeader =
                    readsSparkSource.getHeader(OutputFile.of(PipelineOutput.UNMAPPED, flowCell.lanes().get(0)).path(), reference.path());
            JavaRDD<GATKRead> alignedDuplicatesMarked = MarkDuplicatesSpark.mark(mergedReads,
                    firstHeader,
                    MarkDuplicatesScoringStrategy.SUM_OF_BASE_QUALITIES,
                    new OpticalDuplicateFinder(),
                    1,
                    false);
            writeOutput(context, flowCell, firstHeader, alignedDuplicatesMarked);
        }
    }

    private JavaRDD<GATKRead> merged(final List<JavaRDD<GATKRead>> mergedReadsList) {
        JavaRDD<GATKRead> mergedReads = context.emptyRDD();
        for (JavaRDD<GATKRead> gatkReadJavaRDD : mergedReadsList) {
            mergedReads = mergedReads.union(gatkReadJavaRDD);
        }
        return mergedReads;
    }

    private static Consumer<Lane> createUBAMFromFastQ() {
        return lane -> PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, lane), readFileArgumentOf(2, lane), "SM=" + lane.sample().name(),
                        "O=" + OutputFile.of(PipelineOutput.UNMAPPED, lane).path() }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, Lane lane) {
        return format("F%s=%s/%s_L00%s_R%s.fastq", sampleIndex, lane.sample().directory(), lane.sample().name(), lane.index(), sampleIndex);
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

    private void writeOutput(final JavaSparkContext context, final FlowCell flowCell, SAMFileHeader header,
            final JavaRDD<GATKRead> alignedReads) throws IOException {
        ReadsSparkSink.writeReads(context, OutputFile.of(output(), flowCell).path(), null, alignedReads, header, ReadsWriteFormat.SINGLE);
    }
}
