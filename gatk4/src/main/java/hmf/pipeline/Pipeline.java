package hmf.pipeline;

import static java.lang.String.format;

import static hmf.pipeline.PipelineOutput.ALIGNED;
import static hmf.pipeline.PipelineOutput.SORTED;
import static hmf.pipeline.PipelineOutput.UNMAPPED;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import picard.sam.FastqToSam;
import picard.sam.SortSam;

class Pipeline {

    private static final String RESULTS_DIRECTORY = System.getProperty("user.dir") + "/results";
    private final Configuration configuration;
    private final ReadsSparkSource readsSource;
    private final JavaSparkContext sparkContext;

    Pipeline(JavaSparkContext sparkContext, Configuration configuration) {
        this.configuration = configuration;
        this.readsSource = new ReadsSparkSource(sparkContext);
        this.sparkContext = sparkContext;
    }

    void execute() throws IOException {
        createResultsOutputDirectory();
        String unmappedBamFileName = UNMAPPED.path(configuration.sampleName());
        convertFastQToUnmappedBAM(configuration, unmappedBamFileName);
        runBwa(unmappedBamFileName, bwaSparkEngine(sparkContext, configuration, readsSource, unmappedBamFileName));
        sortBamByCoordinate(configuration);
    }

    private static void sortBamByCoordinate(final Configuration configuration) {
        PicardExecutor.of(new SortSam(),
                new String[] { format("I=%s", ALIGNED.path(configuration.sampleName())),
                        format("O=%s", SORTED.path(configuration.sampleName())), "SORT_ORDER=coordinate" }).execute();
    }

    private static void createResultsOutputDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(RESULTS_DIRECTORY));
        Files.createDirectory(Paths.get(RESULTS_DIRECTORY));
    }

    private void runBwa(final String unmappedBamFileName, final BwaSparkEngine bwaEngine) throws IOException {
        Trace trace = Trace.of(Pipeline.class, "Execution of BwaSpark tool").start();
        JavaRDD<GATKRead> alignedReads =
                bwaEngine.align(readsSource.getParallelReads(unmappedBamFileName, configuration.referencePath()), true);
        writeBwaOutput(bwaEngine, alignedReads);
        trace.finish();
    }

    private void writeBwaOutput(final BwaSparkEngine bwaEngine, final JavaRDD<GATKRead> alignedReads) throws IOException {
        ReadsSparkSink.writeReads(sparkContext, ALIGNED.path(configuration.sampleName()),
                null,
                alignedReads,
                bwaEngine.getHeader(),
                ReadsWriteFormat.SINGLE);
    }

    private static void convertFastQToUnmappedBAM(final Configuration configuration, final String unmappedBamFileName) throws IOException {
        PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, configuration), readFileArgumentOf(2, configuration),
                        "SM=" + configuration.sampleName(), "O=" + unmappedBamFileName }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, Configuration configuration) {
        return format("F%s=%s/%s_R%s.fastq", sampleIndex, configuration.sampleDirectory(), configuration.sampleName(), sampleIndex);
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
}
