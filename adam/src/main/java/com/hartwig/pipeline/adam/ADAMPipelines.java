package com.hartwig.pipeline.adam;

import java.util.List;

import com.hartwig.io.OutputType;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.BamCreationPipeline;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;

public class ADAMPipelines {

    public static BamCreationPipeline bamCreation(final String referenceGenomePath, final List<String> knownIndelPaths,
            final ADAMContext adamContext, final int bwaThreads, final CoverageThreshold... coverageThresholds) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(referenceGenomePath);
        return BamCreationPipeline.builder()
                .qcFactory(coverageThresholds.length == 0
                        ? QC.defaultQC(javaADAMContext, referenceGenome)
                        : QC.qcWith(javaADAMContext, referenceGenome, coverageThresholds))
                .alignment(new ADAMBwa(referenceGenome, adamContext, bwaThreads))
                .alignmentDatasource(new AlignmentRDDSource(OutputType.ALIGNED, javaADAMContext))
                .addBamEnrichment(new ADAMMarkDuplicatesAndSort(javaADAMContext))
                .addBamEnrichment(new ADAMRealignIndels(KnownIndels.of(knownIndelPaths), referenceGenome, javaADAMContext))
                .addBamEnrichment(new ADAMAddMDTags(javaADAMContext, referenceGenome))
                .bamStore(new ADAMBAMStore())
                .build();
    }
}
