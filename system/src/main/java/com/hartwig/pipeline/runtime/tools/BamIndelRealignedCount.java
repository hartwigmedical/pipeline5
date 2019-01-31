package com.hartwig.pipeline.runtime.tools;

import java.util.HashMap;

import com.hartwig.pipeline.runtime.spark.SparkContexts;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.JavaSaveArgs;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BamIndelRealignedCount {

    private final Logger LOGGER = LoggerFactory.getLogger(FastQReadCount.class);

    public void execute() {
        JavaSparkContext context = SparkContexts.create("bam", null, new HashMap<>());
        JavaADAMContext adamContext = new JavaADAMContext(new ADAMContext(context.sc()));
        save(adamContext, "gs://indels/", "COLO829R.p4.bam");
        save(adamContext, "gs://indels/", "COLO829R.p5.bam");
    }

    private void save(final JavaADAMContext adamContext, final String directory, final String bamFile) {
        AlignmentRecordDataset aRD = adamContext.loadAlignments(directory + bamFile);
        JavaRDD<AlignmentRecord> filtered = aRD.rdd()
                .toJavaRDD()
                .filter(record -> record.getOriginalCigar() != null && !record.getCigar().equals(record.getOriginalCigar()));

        LOGGER.info("After filtering BAM [{}] has [{}] reads", bamFile, filtered.count());

        JavaSaveArgs saveArgs = new JavaSaveArgs(directory + bamFile.replace("bam", "realigments.bam"),
                128 * 1024 * 1024,
                1024 * 1024,
                CompressionCodecName.GZIP,
                false,
                true,
                false);
        aRD.replaceRdd(filtered.rdd(), aRD.optPartitionMap()).save(saveArgs, false);
    }

    public static void main(String[] args) {
        new BamIndelRealignedCount().execute();
    }
}
