package com.hartwig.testsupport;

import com.hartwig.support.test.Resources;

import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.RDDBoundAlignmentRecordRDD;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;

import scala.Option;

public class TestRDDs {

    public static AlignmentRecordRDD alignmentRecordRDD(final String bamFile) {
        return javaAdam().loadAlignments(Resources.testResource(bamFile));
    }

    @NotNull
    public static JavaADAMContext javaAdam() {
        return new JavaADAMContext(new ADAMContext(SparkContextSingleton.instance().sc()));
    }

    public static AlignmentRecordRDD emptyAlignnmentRecordRDD() {
        RDD<AlignmentRecord> rdd = SparkContextSingleton.instance().<AlignmentRecord>emptyRDD().rdd();
        return new RDDBoundAlignmentRecordRDD(rdd, null, null, null, Option.empty());
    }
}
