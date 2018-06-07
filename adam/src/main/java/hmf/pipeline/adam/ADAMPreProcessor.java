package hmf.pipeline.adam;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.bdgenomics.adam.api.java.FragmentsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;

import hmf.io.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.sample.FlowCell;
import hmf.sample.Lane;
import hmf.sample.Reference;
import scala.Option;

class ADAMPreProcessor implements Stage<FlowCell> {

    private final ADAMContext adamContext;
    private final Reference reference;

    ADAMPreProcessor(final Reference reference, final ADAMContext adamContext) {
        this.adamContext = adamContext;
        this.reference = reference;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.DUPLICATE_MARKED;
    }

    @Override
    public void execute(FlowCell flowCell) throws IOException {
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(reference.path() + ".dict");
        List<AlignmentRecordRDD> laneRdds =
                flowCell.lanes().stream().map(lane -> adamBwa(sequenceDictionary, lane)).collect(Collectors.toList());
        if (!laneRdds.isEmpty()) {
            laneRdds.get(0).<AlignmentRecordRDD>union(asScalaBufferConverter(laneRdds.subList(1,
                    laneRdds.size())).asScala()).markDuplicates().save(Persistence.defaultSave(flowCell, output()), true);
        }
    }

    private AlignmentRecordRDD adamBwa(final SequenceDictionary sequenceDictionary, final Lane lane) {
        return adamContext.loadInterleavedFastqAsFragments(format("%s/%s_L00%s_interleaved.fastq",
                lane.sample().directory(),
                lane.sample().name(), lane.index()))
                .pipe(BwaCommand.tokens(reference, lane.sample()),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0,
                        InterleavedFASTQInFormatter.class,
                        new AnySAMOutFormatter(),
                        new FragmentsToAlignmentRecordsConverter())
                .replaceRecordGroups(recordDictionary(recordGroup(lane.sample().name())))
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
                Option.empty(), Option.empty(), Option.empty(), Option.apply(sample),
                Option.empty(),
                Option.empty(),
                Option.empty());
    }
}
