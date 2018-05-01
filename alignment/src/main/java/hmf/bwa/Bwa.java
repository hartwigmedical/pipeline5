package hmf.bwa;

import hmf.bwa.BwaConfiguration;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.utils.read.GATKRead;

public class Bwa extends GATKSparkTool{

    private final BwaConfiguration configuration;

    public Bwa(BwaConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void runTool(JavaSparkContext sparkContext) {
        BwaSparkEngine bwaEngine = new BwaSparkEngine(sparkContext, configuration.getReferenceFile(), configuration
                .getIndexFile(), new SAMFileHeader(), new SAMSequenceDictionary());

        JavaRDD<GATKRead> align = bwaEngine.align(getReads(), true);
    }

    public void execute(BwaConfiguration configuration) {

    }
}
