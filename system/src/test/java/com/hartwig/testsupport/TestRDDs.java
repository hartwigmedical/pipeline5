package com.hartwig.testsupport;

import java.util.Arrays;

import com.hartwig.support.test.Resources;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.read.RDDBoundAlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;

import scala.Option;

public class TestRDDs {

    public static AlignmentRecordDataset alignmentRecordDataset(String bamFile, JavaSparkContext sparkContext) {
        return javaAdam(sparkContext).loadAlignments(Resources.testResource(bamFile));
    }

    public static AlignmentRecordDataset alignmentRecordDataset(JavaSparkContext sparkContext, AlignmentRecord... records) {
        return new RDDBoundAlignmentRecordDataset(sparkContext.parallelize(Arrays.asList(records)).rdd(), null, null, null, Option.empty());
    }

    @NotNull
    public static JavaADAMContext javaAdam(JavaSparkContext sparkContext) {
        return new JavaADAMContext(new ADAMContext(sparkContext.sc()));
    }

    public static AlignmentRecordDataset emptyAlignnmentRecordRDD(JavaSparkContext sparkContext) {
        RDD<AlignmentRecord> rdd = sparkContext.<AlignmentRecord>emptyRDD().rdd();
        return new RDDBoundAlignmentRecordDataset(rdd, null, null, null, Option.empty());
    }
}
