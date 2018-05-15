package hmf.pipeline.gatk;

import static hmf.pipeline.PipelineOutput.ALIGNED;

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

import hmf.pipeline.Configuration;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.pipeline.Trace;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;

public class BwaSpark implements Stage {

    private final ReadsSparkSource readsSparkSource;
    private final JavaSparkContext context;
    private final Configuration configuration;

    BwaSpark(final Configuration configuration, final JavaSparkContext context) {
        this.readsSparkSource = new ReadsSparkSource(context);
        this.configuration = configuration;
        this.context = context;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.ALIGNED;
    }

    @Override
    public void execute() throws IOException {
        String unmappedBamFileName = PipelineOutput.UNMAPPED.path(configuration.sampleName());
        runBwa(unmappedBamFileName, bwaSparkEngine(context, configuration, readsSparkSource, unmappedBamFileName));
    }

    private void runBwa(final String unmappedBamFileName, final BwaSparkEngine bwaEngine) throws IOException {
        Trace trace = Trace.of(BwaSpark.class, "Execution of BwaSpark tool").start();
        writeBwaOutput(context,
                configuration,
                bwaEngine,
                bwaEngine.align(readsSparkSource.getParallelReads(unmappedBamFileName, configuration.referencePath()), true));
        trace.finish();
    }

    private static BwaSparkEngine bwaSparkEngine(final JavaSparkContext sparkContext, final Configuration configuration,
            final ReadsSparkSource readsSource, final String unmappedBamFileName) throws IOException {
        SAMFileHeader header = readsSource.getHeader(unmappedBamFileName, configuration.referencePath());
        SAMSequenceDictionary dictionary = dictionary(configuration.referencePath(), header);
        return new BwaSparkEngine(sparkContext, configuration.referencePath(), null, header, dictionary);
    }

    private static SAMSequenceDictionary dictionary(final String referenceFile, final SAMFileHeader readsHeader) throws IOException {
        return new ReferenceMultiSource(referenceFile, ReferenceWindowFunctions.IDENTITY_FUNCTION).getReferenceSequenceDictionary(
                readsHeader.getSequenceDictionary());
    }

    private static void writeBwaOutput(final JavaSparkContext context, final Configuration configuration, final BwaSparkEngine bwaEngine,
            final JavaRDD<GATKRead> alignedReads) throws IOException {
        ReadsSparkSink.writeReads(context,
                ALIGNED.path(configuration.sampleName()),
                null,
                alignedReads,
                bwaEngine.getHeader(),
                ReadsWriteFormat.SINGLE);
    }
}
