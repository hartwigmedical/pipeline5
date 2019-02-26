package com.hartwig.testsupport;

import com.hartwig.support.test.Resources;

import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.read.RDDBoundAlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;

import scala.Option;

public class TestRDDs {

    public static AlignmentRecordDataset alignmentRecordDataset(final String bamFile) {
        return javaAdam().loadAlignments(Resources.testResource(bamFile));
    }

    @NotNull
    public static JavaADAMContext javaAdam() {
        return new JavaADAMContext(new ADAMContext(SparkContextSingleton.instance().sc()));
    }

    public static AlignmentRecordDataset emptyAlignnmentRecordRDD() {
        RDD<AlignmentRecord> rdd = SparkContextSingleton.instance().<AlignmentRecord>emptyRDD().rdd();
        return new RDDBoundAlignmentRecordDataset(rdd, null, null, null, Option.empty());
    }
}
