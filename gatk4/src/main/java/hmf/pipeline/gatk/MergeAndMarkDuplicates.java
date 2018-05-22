package hmf.pipeline.gatk;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.tools.spark.transforms.markduplicates.MarkDuplicatesSpark;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.read.markduplicates.MarkDuplicatesScoringStrategy;

import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.sample.FlowCell;
import hmf.sample.Lane;
import htsjdk.samtools.SAMFileHeader;

class MergeAndMarkDuplicates implements Stage<FlowCell> {

    private final ReadsSparkSource sparkSource;
    private final JavaSparkContext javaSparkContext;

    MergeAndMarkDuplicates(final JavaSparkContext javaSparkContext) {
        this.sparkSource = new ReadsSparkSource(javaSparkContext);
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.DEDUPED;
    }

    @Override
    public void execute(FlowCell flowCell) throws IOException {
        JavaRDD<GATKRead> mergedReads = javaSparkContext.emptyRDD();
        SAMFileHeader header = new SAMFileHeader();
        for (Lane lane : flowCell.lanes()) {
            String sortedBamFileName = PipelineOutput.SORTED.path(lane);
            header = sparkSource.getHeader(sortedBamFileName, null);
            mergedReads = mergedReads.union(sparkSource.getParallelReads(sortedBamFileName, null));
        }
        JavaRDD<GATKRead> markedDuplicates =
                MarkDuplicatesSpark.mark(mergedReads, header, MarkDuplicatesScoringStrategy.SUM_OF_BASE_QUALITIES, null, 1, false);
        ReadsSparkSink.writeReads(javaSparkContext, output().path(flowCell), null, markedDuplicates, header, ReadsWriteFormat.SINGLE);
    }
}
