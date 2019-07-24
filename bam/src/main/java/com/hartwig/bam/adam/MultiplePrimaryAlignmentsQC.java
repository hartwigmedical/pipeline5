package com.hartwig.bam.adam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.bam.QCResult;
import com.hartwig.bam.QualityControl;
import com.hartwig.io.InputOutput;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.bdgenomics.formats.avro.Fragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiplePrimaryAlignmentsQC implements QualityControl<AlignmentRecordDataset>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiplePrimaryAlignmentsQC.class);

    @Override
    public QCResult check(final InputOutput<AlignmentRecordDataset> toQC) {
        List<MultiplePrimaryAlignments> errors = toQC.payload().toFragments().rdd().toJavaRDD().flatMap(fragment -> {
            List<MultiplePrimaryAlignments> multiplePrimaryAlignments = new ArrayList<>();

            findMultiplePrimaryAlignments(fragment,
                    multiplePrimaryAlignments,
                    findReadInFragment(fragment, 0),
                    MultiplePrimaryAlignments.ReadOrdinal.FIRST);
            findMultiplePrimaryAlignments(fragment,
                    multiplePrimaryAlignments,
                    findReadInFragment(fragment, 1),
                    MultiplePrimaryAlignments.ReadOrdinal.SECOND);

            return multiplePrimaryAlignments.iterator();
        }).collect();

        if (!errors.isEmpty()) {
            for (MultiplePrimaryAlignments multiplePrimaryAlignments : errors) {
                LOGGER.error("{}", multiplePrimaryAlignments);
            }
            return QCResult.failure("Multiple primary alignments. See driver logs for individual alignments");
        }
        return QCResult.ok();
    }

    private void findMultiplePrimaryAlignments(final Fragment fragment, final List<MultiplePrimaryAlignments> multiplePrimaryAlignments,
            final List<AlignmentRecord> alignmentRecords, final MultiplePrimaryAlignments.ReadOrdinal ordinal) {
        long numPrimaryAlignments = primaryAlignmentCount(alignmentRecords);
        if (numPrimaryAlignments > 1) {
            multiplePrimaryAlignments.add(MultiplePrimaryAlignments.of(fragment.getName(),
                    ordinal,
                    numPrimaryAlignments,
                    alignmentRecords.size()));
        }
    }

    private List<AlignmentRecord> findReadInFragment(final Fragment fragment, final int readInFragment) {
        return fragment.getAlignments()
                .stream()
                .filter(AlignmentRecord::getReadMapped)
                .filter(AlignmentRecord::getMateMapped)
                .filter(AlignmentRecord::getProperPair)
                .filter(alignment -> !alignment.getDuplicateRead())
                .filter(alignment -> alignment.getReadInFragment() == readInFragment)
                .filter(alignment -> !alignment.getSupplementaryAlignment())
                .collect(Collectors.toList());
    }

    private long primaryAlignmentCount(final List<AlignmentRecord> alignments) {
        return alignments.stream().filter(AlignmentRecord::getPrimaryAlignment).count();
    }
}
