package com.hartwig.pipeline.adam;

import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.exception.Exceptions;
import com.hartwig.io.Output;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Lane;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.AlignmentRecordsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;
import org.bdgenomics.adam.rdd.read.FASTQInFormatter;

import htsjdk.samtools.ValidationStringency;
import scala.Option;

class ADAMPreProcessor implements Stage<Sample, AlignmentRecordRDD> {

    private final ADAMContext adamContext;
    private final ReferenceGenome referenceGenome;

    ADAMPreProcessor(final ReferenceGenome referenceGenome, final ADAMContext adamContext) {
        this.adamContext = adamContext;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public Output<Sample, AlignmentRecordRDD> execute(Sample sample) throws IOException {
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(referenceGenome.path() + ".dict");
        List<AlignmentRecordRDD> laneRdds =
                sample.lanes().stream().map(lane -> adamBwa(sequenceDictionary, sample, lane)).collect(Collectors.toList());
        if (!laneRdds.isEmpty()) {
            return Output.of(OutputType.DUPLICATE_MARKED,
                    sample,
                    laneRdds.get(0).<AlignmentRecordRDD>union(asScalaBufferConverter(laneRdds.subList(1,
                            laneRdds.size())).asScala()).markDuplicates());
        }
        throw Exceptions.noLanesInSample();
    }

    private AlignmentRecordRDD adamBwa(final SequenceDictionary sequenceDictionary, final Sample sample, final Lane lane) {
        return adamContext.loadFastq(lane.matesFile(), Option.empty(), Option.empty(), ValidationStringency.LENIENT)
                .pipe(BwaCommand.tokens(referenceGenome, sample, lane),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0,
                        FASTQInFormatter.class,
                        new AnySAMOutFormatter(),
                        new AlignmentRecordsToAlignmentRecordsConverter())
                .replaceRecordGroups(recordDictionary(recordGroup(sample.name())))
                .replaceSequences(sequenceDictionary);
    }

    private RecordGroupDictionary recordDictionary(final RecordGroup recordGroup) {
        return new RecordGroupDictionary(asScalaBufferConverter(singletonList(recordGroup)).asScala());
    }

    private RecordGroup recordGroup(final String sample) {
        return new RecordGroup(sample,
                sample,
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.apply(sample),
                Option.empty(),
                Option.empty(),
                Option.empty());
    }
}
