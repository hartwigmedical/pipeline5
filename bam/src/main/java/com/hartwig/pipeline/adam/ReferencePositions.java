package com.hartwig.pipeline.adam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bdgenomics.formats.avro.AlignmentRecord;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.TextCigarCodec;
import scala.Tuple3;

class ReferencePositions {

    static int getReadPositionAtReferencePosition(final AlignmentRecord record, final int pos) {
        if (pos <= 0) {
            return 0;
        }
        List<Tuple3<Integer, Integer, Integer>> alignmentBlocks =
                getAlignmentStarts(TextCigarCodec.decode(record.getCigar()), record.getStart().intValue());

        for (final Tuple3<Integer, Integer, Integer> alignmentBlock : alignmentBlocks) {
            if (getEnd(alignmentBlock._2(), alignmentBlock._3()) >= pos) {
                if (pos < alignmentBlock._2()) {
                    //There must have been a deletion block that skipped
                    return 0;
                } else {
                    return pos - alignmentBlock._2() + alignmentBlock._1();
                }
            }
        }
        // (PAWO): if we are here, the reference position was not overlapping the read at all
        return 0;
    }

    private static long getEnd(final long start, final int length) {
        return start + length - 1;
    }

    private static List<Tuple3<Integer, Integer, Integer>> getAlignmentStarts(final Cigar cigar, final int alignmentStart) {
        if (cigar == null) {
            return Collections.emptyList();
        }

        final List<Tuple3<Integer, Integer, Integer>> alignmentBlocks = new ArrayList<>();
        int readBase = 0;
        int refBase = alignmentStart;

        for (final CigarElement e : cigar.getCigarElements()) {
            switch (e.getOperator()) {
                case H:
                    break; // ignore hard clips
                case P:
                    break; // ignore pads
                case S:
                    readBase += e.getLength();
                    break; // soft clip read bases
                case N:
                    refBase += e.getLength();
                    break;  // reference skip
                case D:
                    refBase += e.getLength();
                    break;
                case I:
                    readBase += e.getLength();
                    break;
                case M:
                case EQ:
                case X:
                    final int length = e.getLength();
                    alignmentBlocks.add(Tuple3.apply(readBase, refBase, length));
                    readBase += length;
                    refBase += length;
                    break;
                default:
                    throw new IllegalStateException("Case statement didn't deal with op: " + e.getOperator() + "in CIGAR: " + cigar);
            }
        }
        return Collections.unmodifiableList(alignmentBlocks);
    }
}
