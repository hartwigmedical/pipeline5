package com.hartwig.pipeline.adam;

import static java.util.Collections.singletonList;

import static scala.collection.JavaConverters.asScalaBufferConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.exception.Exceptions;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Lane;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.AlignmentStage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.api.java.FragmentsToAlignmentRecordsConverter;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.fragment.FragmentDataset;
import org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.read.AnySAMOutFormatter;

import htsjdk.samtools.ValidationStringency;
import scala.Option;

class Bwa implements AlignmentStage {

    private final ADAMContext adamContext;
    private final ReferenceGenome referenceGenome;
    private final FileSystem fileSystem;
    private final int bwaThreads;

    Bwa(final ReferenceGenome referenceGenome, final ADAMContext adamContext, final FileSystem fileSystem, int bwaThreads) {
        this.adamContext = adamContext;
        this.referenceGenome = referenceGenome;
        this.fileSystem = fileSystem;
        this.bwaThreads = bwaThreads;
    }

    @Override
    public OutputType outputType() {
        return OutputType.ALIGNED;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(InputOutput<AlignmentRecordDataset> input) throws IOException {
        SequenceDictionary sequenceDictionary = adamContext.loadSequenceDictionary(referenceGenome.path() + ".dict");
        Sample sample = input.sample();
        List<AlignmentRecordDataset> laneRdds =
                sample.lanes().parallelStream().map(lane -> adamBwa(sequenceDictionary, sample, lane)).collect(Collectors.toList());
        if (!laneRdds.isEmpty()) {
            return InputOutput.of(outputType(),
                    sample,
                    laneRdds.get(0).<AlignmentRecordDataset>union(asScalaBufferConverter(laneRdds.subList(1, laneRdds.size())).asScala()));
        }
        throw Exceptions.noLanesInSample();
    }

    private AlignmentRecordDataset adamBwa(final SequenceDictionary sequenceDictionary, final Sample sample, final Lane lane) {
        FragmentDataset FragmentDataset = adamContext.loadPairedFastq(lane.readsPath(),
                lane.matesPath(),
                Option.empty(), Option.apply(StorageLevel.DISK_ONLY()), ValidationStringency.STRICT).toFragments();
        initializeBwaSharedMemoryPerExecutor(FragmentDataset);
        return RDDs.persistDisk(RDDs.AlignmentRecordDataset(((FragmentDataset) FragmentDataset).pipe(BwaCommand.tokens(referenceGenome,
                sample,
                lane,
                bwaThreads),
                new ArrayList<>(),
                Collections.emptyMap(),
                0,
                InterleavedFASTQInFormatter.class,
                new AnySAMOutFormatter(),
                new FragmentsToAlignmentRecordsConverter())
                .replaceRecordGroups(recordDictionary(recordGroup(sample, lane))).replaceSequences(sequenceDictionary)));
    }

    private void initializeBwaSharedMemoryPerExecutor(final FragmentDataset FragmentDataset) {
        for (String file : IndexFiles.resolve(fileSystem, referenceGenome)) {
            adamContext.sc().addFile(file);
        }
        final String path = referenceGenome.path();
        FragmentDataset.jrdd().foreach(fragment -> InitializeBwaSharedMemory.run(SparkFiles.get(new Path(path).getName())));
    }

    private RecordGroupDictionary recordDictionary(final RecordGroup recordGroup) {
        return new RecordGroupDictionary(asScalaBufferConverter(singletonList(recordGroup)).asScala());
    }

    private RecordGroup recordGroup(final Sample sample, final Lane lane) {
        return new RecordGroup(sample.name(),
                lane.recordGroupId(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                Option.apply(sample.name()),
                Option.empty(),
                Option.apply("ILLUMINA"),
                Option.apply(lane.flowCellId()));
    }
}
