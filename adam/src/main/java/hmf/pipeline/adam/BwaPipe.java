package hmf.pipeline.adam;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.Collections;

import org.bdgenomics.adam.api.java.FragmentsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.fragment.FragmentRDD;
import org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;

import hmf.pipeline.Configuration;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;
import scala.Option;

class BwaPipe implements Stage {

    private final ADAMContext adamContext;
    private final Configuration configuration;

    BwaPipe(final Configuration configuration, final ADAMContext adamContext) {
        this.adamContext = adamContext;
        this.configuration = configuration;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.ALIGNED;
    }

    @Override
    public void execute() throws IOException {
        FragmentRDD fragmentRDD = adamContext.loadInterleavedFastqAsFragments(format("%s/%s_interleaved.fastq",
                configuration.sampleDirectory(),
                configuration.sampleName()));
        AlignmentRecordRDD aligned = fragmentRDD.pipe(BwaCommand.tokens(configuration),
                Collections.emptyList(),
                Collections.emptyMap(),
                0,
                InterleavedFASTQInFormatter.class,
                new AnySAMOutFormatter(),
                new FragmentsToAlignmentRecordsConverter());
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(configuration.referencePath() + ".dict");
        aligned.replaceRecordGroups(recordDictionary(recordGroup(configuration.sampleName())))
                .replaceSequences(sequenceDictionary)
                .save(SaveArgs.defaultSave(configuration, output()), false);
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
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty());
    }
}
