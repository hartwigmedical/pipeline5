package com.hartwig.pipeline.adam;

import static com.hartwig.pipeline.adam.CoverageThreshold.of;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.QualityControl;
import com.hartwig.pipeline.QualityControlFactory;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;

public class QC {

    static QualityControlFactory defaultQC(final JavaADAMContext javaADAMContext, final ReferenceGenome referenceGenome) {
        return qcWith(javaADAMContext, referenceGenome, of(10, 90), of(20, 70));
    }

    @NotNull
    static QualityControlFactory qcWith(final JavaADAMContext javaADAMContext, final ReferenceGenome referenceGenome,
            final CoverageThreshold... coverageThresholds) {
        return new QualityControlFactory() {
            @Override
            public QualityControl<AlignmentRecordRDD> readCount(final AlignmentRecordRDD initial) {
                return ADAMReadCountCheck.from(initial);
            }

            @Override
            public QualityControl<AlignmentRecordRDD> referenceBAMQC() {
                return ADAMFinalBAMQC.of(javaADAMContext, referenceGenome, coverageThresholds);
            }
        };
    }
}
