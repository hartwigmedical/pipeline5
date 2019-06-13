package com.hartwig.pipeline.adam;

import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.InputOutput;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.spark.SparkContexts;
import com.hartwig.testsupport.TestRDDs;

import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class MultiplePrimaryAlignmentsQCTest {

    private final static JavaSparkContext SPARK_CONTEXT = SparkContexts.create("multiple-primary-alignment-qc-test", HUNDREDK_READS_HISEQ);
    private MultiplePrimaryAlignmentsQC victim;

    @Before
    public void setUp() throws Exception {
        victim = new MultiplePrimaryAlignmentsQC();
    }

    @AfterClass
    public static void afterClass() {
        SPARK_CONTEXT.close();
    }

    @Test
    public void emptyRddInputReturnsOk() {
        assertThat(victim.check(io(TestRDDs.emptyAlignmentRecordRDD(SPARK_CONTEXT))).isOk()).isTrue();
    }

    @Test
    public void singlePrimaryAlignmentOnBothSidesOfReadReturnsOk() {
        AlignmentRecord firstOfPairPrimary = primaryAlignmentRecord("test", true, true, true, false, false, 0);
        AlignmentRecord secondOfPairPrimary = primaryAlignmentRecord("test", true, true, true, false, false, 1);
        checkAlignmentsPassQC(firstOfPairPrimary, secondOfPairPrimary);
    }

    @Test
    public void multiplePrimaryAlignmentsOnFirstOfPair() {
        AlignmentRecord firstOfPairPrimary = primaryAlignmentRecord("test", true, true, true, false, false, 0);
        AlignmentRecord firstOfPairSecondPrimary = primaryAlignmentRecord("test", true, true, true, false, false, 0);
        checkAlignmentsFailQC(firstOfPairPrimary, firstOfPairSecondPrimary);
    }

    @Test
    public void alignmentsGroupedAndSeparatedByReadName() {
        AlignmentRecord firstOfPairPrimary = primaryAlignmentRecord("test1", true, true, true, false, false, 0);
        AlignmentRecord firstOfPairSecondPrimary = primaryAlignmentRecord("test2", true, true, true, false, false, 0);
        checkAlignmentsPassQC(firstOfPairPrimary, firstOfPairSecondPrimary);
    }

    @Test
    public void multiplePrimaryAlignmentsOnSecondOfPair() {
        AlignmentRecord secondOfPairPrimary = primaryAlignmentRecord("test", true, true, true, false, false, 1);
        AlignmentRecord secondOfPairSecondPrimary = primaryAlignmentRecord("test", true, true, true, false, false, 1);
        checkAlignmentsFailQC(secondOfPairPrimary, secondOfPairSecondPrimary);
    }

    @Test
    public void unmappedReadsFiltered() {
        checkAlignmentsPassQC(primaryAlignmentRecord("test", true, true, true, false, false, 0),
                primaryAlignmentRecord("test", false, true, true, false, false, 0));
    }

    @Test
    public void unmappedMatesFiltered() {
        checkAlignmentsPassQC(primaryAlignmentRecord("test", true, true, true, false, false, 0),
                primaryAlignmentRecord("test", true, false, true, false, false, 0));
    }

    @Test
    public void notProperPairFiltered() {
        checkAlignmentsPassQC(primaryAlignmentRecord("test", true, true, true, false, false, 0),
                primaryAlignmentRecord("test", true, true, false, false, false, 0));
    }

    @Test
    public void duplicatesFiltered() {
        checkAlignmentsPassQC(primaryAlignmentRecord("test", true, true, true, false, false, 0),
                primaryAlignmentRecord("test", true, true, true, true, false, 0));
    }

    @Test
    public void supplementaryFiltered() {
        checkAlignmentsPassQC(primaryAlignmentRecord("test", true, true, true, false, false, 0),
                primaryAlignmentRecord("test", true, true, true, false, true, 0));
    }

    private void checkAlignmentsPassQC(final AlignmentRecord firstOfPairPrimary, final AlignmentRecord firstOfPairSecondPrimary) {
        assertThat(victim.check(io(TestRDDs.alignmentRecordDataset(SPARK_CONTEXT, firstOfPairPrimary, firstOfPairSecondPrimary)))
                .isOk()).isTrue();
    }

    private void checkAlignmentsFailQC(final AlignmentRecord firstOfPairPrimary, final AlignmentRecord firstOfPairSecondPrimary) {
        assertThat(victim.check(io(TestRDDs.alignmentRecordDataset(SPARK_CONTEXT, firstOfPairPrimary, firstOfPairSecondPrimary)))
                .isOk()).isFalse();
    }

    @NotNull
    private AlignmentRecord primaryAlignmentRecord(final String name, final boolean readMapped, final boolean mateMapped,
            final boolean properPair, final boolean duplicate, final boolean supplementary, final int readInFragment) {
        AlignmentRecord record = new AlignmentRecord();
        record.setReadName(name);
        record.setReadMapped(readMapped);
        record.setMateMapped(mateMapped);
        record.setProperPair(properPair);
        record.setPrimaryAlignment(true);
        record.setReadInFragment(readInFragment);
        record.setDuplicateRead(duplicate);
        record.setSupplementaryAlignment(supplementary);
        return record;
    }

    @NotNull
    private static InputOutput<AlignmentRecordDataset> io(final AlignmentRecordDataset alignmentRecordDataset) {
        return InputOutput.of(Sample.builder("director", "sample").build(), alignmentRecordDataset);
    }
}