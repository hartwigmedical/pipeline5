package hmf.pipeline;

import static java.lang.String.format;

import static htsjdk.samtools.SAMFileHeader.SortOrder.queryname;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import picard.sam.FastqToSam;

class Pipeline {

    private static final String UNMAPPED_BAM = format("%s/target/01_unmapped.bam", workingDirectory());
    private static final String QUERYNAME_SORTED_BAM = format("%s/target/02_queryname_sorted.bam", workingDirectory());

    private final Configuration configuration;
    private final ReadsSparkSource readsSource;
    private final BwaSparkEngine bwaEngine;

    Pipeline(JavaSparkContext sparkContext, Configuration configuration) {
        this.configuration = configuration;
        this.readsSource = new ReadsSparkSource(sparkContext);
        this.bwaEngine = bwaSparkEngine(sparkContext, configuration);
    }

    List<GATKRead> execute() {
        convertFastQToUnmappedBAM(configuration);
        return bwaEngine.align(readsSource.getParallelReads(QUERYNAME_SORTED_BAM, configuration.getReferenceFile()), true).collect();
    }

    private static void convertFastQToUnmappedBAM(final Configuration configuration) {
        PicardExecutor.of(new FastqToSam(),
                new String[] { readFileArgumentOf(1, configuration), readFileArgumentOf(2, configuration),
                        "SM=" + configuration.getSampleName(), "O=" + UNMAPPED_BAM }).execute();
    }

    private static String readFileArgumentOf(int sampleIndex, Configuration configuration) {
        return format("F%s=%s/%s_R%s_001.fastq.gz", sampleIndex, configuration.getSamplePath(), configuration.getSampleName(), sampleIndex);
    }

    private static String workingDirectory() {
        return System.getProperty("user.dir");
    }

    private static BwaSparkEngine bwaSparkEngine(final JavaSparkContext sparkContext, final Configuration configuration) {
        SAMFileHeader inputHeader = new SAMFileHeader();
        inputHeader.setSortOrder(queryname);
        return new BwaSparkEngine(sparkContext, configuration.getReferenceFile(), null, inputHeader, new SAMSequenceDictionary());
    }
}
