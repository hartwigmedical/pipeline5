package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.immutables.value.Value;

import htsjdk.samtools.SAMFileHeader;

@Value.Immutable
public interface ReadsAndHeader {

    @Value.Parameter
    JavaRDD<GATKRead> reads();

    @Value.Parameter
    SAMFileHeader header();

    static ReadsAndHeader of(JavaRDD<GATKRead> reads, SAMFileHeader header) {
        return ImmutableReadsAndHeader.of(reads, header);
    }
}
