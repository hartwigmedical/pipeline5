package com.hartwig.pipeline;

import com.esotericsoftware.kryo.Kryo;

import org.apache.spark.serializer.KryoRegistrator;
import org.bdgenomics.adam.models.RecordGroup;
import org.bdgenomics.adam.models.RecordGroupDictionary;
import org.bdgenomics.adam.models.ReferencePosition;
import org.bdgenomics.adam.models.SAMFileHeaderWritable;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.models.SequenceRecord;
import org.bdgenomics.adam.rdd.read.ReferencePositionPair;
import org.bdgenomics.adam.rdd.read.SingleReadBucket;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.bdgenomics.formats.avro.Strand;

public class ADAMKryo implements KryoRegistrator {

    public ADAMKryo() {
    }

    @Override
    public void registerClasses(final Kryo kryo) {
        try {
            kryo.register(AlignmentRecord.class);
            kryo.register(AlignmentRecord[].class);
            kryo.register(ReferencePosition.class);
            kryo.register(ReferencePosition[].class);
            kryo.register(ReferencePositionPair.class);
            kryo.register(Strand.class);
            kryo.register(SingleReadBucket.class);
            kryo.register(SAMFileHeaderWritable.class);
            kryo.register(RecordGroupDictionary.class);
            kryo.register(RecordGroup.class);
            kryo.register(SequenceDictionary.class);
            kryo.register(SequenceRecord.class);
            kryo.register(org.bdgenomics.adam.models.SequenceRecord[].class);
            kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1", false, getClass().getClassLoader()));
            kryo.register(Class.class);
            kryo.register(org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage.class);
            kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$", false, getClass().getClassLoader()));
            kryo.register(org.bdgenomics.adam.converters.FastaConverter.FastaDescriptionLine.class);
            kryo.register(org.bdgenomics.formats.avro.NucleotideContigFragment.class);
            kryo.register(org.bdgenomics.adam.models.IndelTable.class);
            kryo.register(org.bdgenomics.adam.util.ReferenceContigMap.class);
            kryo.register(org.bdgenomics.adam.rdd.read.realignment.TargetSet.class);
            kryo.register(scala.collection.immutable.TreeSet.class);
            kryo.register(org.bdgenomics.adam.rdd.read.realignment.TargetOrdering$.class);
            kryo.register(scala.collection.immutable.RedBlackTree.BlackTree.class);
            kryo.register(scala.collection.immutable.RedBlackTree.RedTree.class);
            kryo.register(org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget.class);
            kryo.register(org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget[].class);
            kryo.register(org.bdgenomics.adam.models.ReferenceRegion.class);
            kryo.register(org.bdgenomics.adam.rich.RichAlignmentRecord.class);
            kryo.register(org.bdgenomics.adam.rich.RichAlignmentRecord[].class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
