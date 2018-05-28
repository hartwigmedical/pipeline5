package hmf.pipeline.gatk;

import static hmf.io.PipelineOutput.ALIGNED;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.pipeline.Trace;
import hmf.sample.Lane;
import hmf.sample.Reference;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;

public class BwaSpark implements Stage<Lane> {

    private final ReadsSparkSource readsSparkSource;
    private final JavaSparkContext context;
    private final Reference reference;

    BwaSpark(final Reference reference, final JavaSparkContext context) {
        this.readsSparkSource = new ReadsSparkSource(context);
        this.context = context;
        this.reference = reference;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.ALIGNED;
    }

    @Override
    public void execute(Lane lane) throws IOException {
        String unmappedBamFileName = OutputFile.of(PipelineOutput.UNMAPPED, lane).path();
        runBwa(unmappedBamFileName, lane, bwaSparkEngine(context, reference, readsSparkSource, unmappedBamFileName));
    }

    private void runBwa(final String unmappedBamFileName, final Lane lane, final BwaSparkEngine bwaEngine) throws IOException {
        Trace trace = Trace.of(BwaSpark.class, "Execution of BwaSpark tool").start();
        writeBwaOutput(context,
                lane,
                bwaEngine,
                bwaEngine.align(readsSparkSource.getParallelReads(unmappedBamFileName, reference.path()), true));
        trace.finish();
    }

    private static BwaSparkEngine bwaSparkEngine(final JavaSparkContext sparkContext, final Reference reference,
            final ReadsSparkSource readsSource, final String unmappedBamFileName) throws IOException {
        SAMFileHeader header = readsSource.getHeader(unmappedBamFileName, reference.path());
        SAMSequenceDictionary dictionary = dictionary(reference.path(), header);
        return new BwaSparkEngine(sparkContext, reference.path(), null, header, dictionary);
    }

    private static SAMSequenceDictionary dictionary(final String referenceFile, final SAMFileHeader readsHeader) throws IOException {
        return new ReferenceMultiSource(referenceFile, ReferenceWindowFunctions.IDENTITY_FUNCTION).getReferenceSequenceDictionary(
                readsHeader.getSequenceDictionary());
    }

    private static void writeBwaOutput(final JavaSparkContext context, final Lane lane, final BwaSparkEngine bwaEngine,
            final JavaRDD<GATKRead> alignedReads) throws IOException {
        ReadsSparkSink.writeReads(context, OutputFile.of(ALIGNED, lane).path(),
                null,
                alignedReads,
                bwaEngine.getHeader(),
                ReadsWriteFormat.SINGLE);
    }
}
