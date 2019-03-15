package com.hartwig.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeSet;

import com.esotericsoftware.kryo.Kryo;
import com.hartwig.pipeline.adam.PositionRangeKey;

import org.apache.avro.generic.GenericData;
import org.apache.spark.internal.io.FileCommitProtocol;
import org.apache.spark.serializer.KryoRegistrator;
import org.bdgenomics.adam.algorithms.consensus.Consensus;
import org.bdgenomics.adam.converters.FastaConverter;
import org.bdgenomics.adam.models.IndelTable;
import org.bdgenomics.adam.models.ReadGroup;
import org.bdgenomics.adam.models.ReadGroupDictionary;
import org.bdgenomics.adam.models.ReferencePosition;
import org.bdgenomics.adam.models.ReferenceRegion;
import org.bdgenomics.adam.models.SAMFileHeaderWritable;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.models.SequenceRecord;
import org.bdgenomics.adam.models.SnpTable;
import org.bdgenomics.adam.models.SnpTableSerializer;
import org.bdgenomics.adam.rdd.read.ReferencePositionPair;
import org.bdgenomics.adam.rdd.read.SingleReadBucket;
import org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget;
import org.bdgenomics.adam.rdd.read.realignment.TargetOrdering$;
import org.bdgenomics.adam.rdd.read.realignment.TargetSet;
import org.bdgenomics.adam.rdd.read.recalibration.CovariateKey;
import org.bdgenomics.adam.rdd.read.recalibration.Observation;
import org.bdgenomics.adam.rich.RichAlignmentRecord;
import org.bdgenomics.adam.serialization.AvroSerializer;
import org.bdgenomics.adam.util.ReferenceContigMap;
import org.bdgenomics.formats.avro.AlignmentRecord;
import org.bdgenomics.formats.avro.Fragment;
import org.bdgenomics.formats.avro.NucleotideContigFragment;
import org.bdgenomics.formats.avro.ProcessingStep;
import org.bdgenomics.formats.avro.Strand;
import org.bdgenomics.formats.avro.Variant;
import org.bdgenomics.formats.avro.VariantAnnotation;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMBinaryTagAndValue;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.ValidationStringency;
import scala.collection.convert.Wrappers$;
import scala.collection.immutable.RedBlackTree;

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
            kryo.register(ReadGroupDictionary.class);
            kryo.register(ReadGroup.class);
            kryo.register(SequenceDictionary.class);
            kryo.register(SequenceRecord.class);
            kryo.register(SequenceRecord[].class);
            kryo.register(Fragment.class);
            kryo.register(Fragment[].class);
            kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1", false, getClass().getClassLoader()));
            kryo.register(Class.class);
            kryo.register(FileCommitProtocol.TaskCommitMessage.class);
            kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$", false, getClass().getClassLoader()));
            kryo.register(FastaConverter.FastaDescriptionLine.class);
            kryo.register(NucleotideContigFragment.class);
            kryo.register(IndelTable.class);
            kryo.register(ReferenceContigMap.class);
            kryo.register(TargetSet.class);
            kryo.register(TreeSet.class);
            kryo.register(TargetOrdering$.class);
            kryo.register(RedBlackTree.BlackTree.class);
            kryo.register(RedBlackTree.RedTree.class);
            kryo.register(IndelRealignmentTarget.class);
            kryo.register(IndelRealignmentTarget[].class);
            kryo.register(ReferenceRegion.class);
            kryo.register(RichAlignmentRecord.class);
            kryo.register(RichAlignmentRecord[].class);
            kryo.register(SAMRecord.class);
            kryo.register(SAMBinaryTagAndValue.class);
            kryo.register(SAMFileHeader.class);
            kryo.register(SAMFileHeader.GroupOrder.class);
            kryo.register(SAMReadGroupRecord.class);
            kryo.register(SAMSequenceDictionary.class);
            kryo.register(SAMSequenceRecord.class);
            kryo.register(SAMFileHeader.SortOrder.class);
            kryo.register(ValidationStringency.class);
            kryo.register(LinkedHashMap.class);
            kryo.register(ArrayList.class);
            kryo.register(HashMap.class);
            kryo.register(Wrappers$.class);
            kryo.register(scala.collection.immutable.TreeSet.class);
            kryo.register(Class.forName("scala.reflect.ManifestFactory$$anon$2", false, getClass().getClassLoader()));
            kryo.register(Object.class);
            kryo.register(Object[].class);
            kryo.register(Double.class);
            kryo.register(Double[].class);
            kryo.register(PositionRangeKey.class);
            kryo.register(Cigar.class);
            kryo.register(CigarElement.class);
            kryo.register(CigarOperator.class);
            kryo.register(Consensus.class);
            kryo.register(Consensus[].class);
            kryo.register(ProcessingStep.class);

            kryo.register(SnpTable.class, new SnpTableSerializer());
            kryo.register(CovariateKey.class);
            kryo.register(Observation.class);
            kryo.register(Class.forName("scala.collection.immutable.MapLike$$anon$2", false, getClass().getClassLoader()));
            kryo.register(Class.forName("org.bdgenomics.adam.models.SnpTable$$anonfun$1", false, getClass().getClassLoader()));
            kryo.register(Variant.class, new AvroSerializer<Variant>(scala.reflect.ClassTag$.MODULE$.apply(Variant.class)));
            kryo.register(VariantAnnotation.class);
            kryo.register(GenericData.Array.class);
            kryo.register(Class.forName("com.google.common.collect.SingletonImmutableList", false, getClass().getClassLoader()));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
