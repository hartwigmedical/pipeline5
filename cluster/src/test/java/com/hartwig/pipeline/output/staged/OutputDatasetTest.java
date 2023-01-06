package com.hartwig.pipeline.output.staged;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.api.model.Dataset;
import com.hartwig.api.model.DatasetFile;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.output.OutputDataset;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class OutputDatasetTest {

    private static final String AMBER_BAF = "amber.baf";
    private static final String TUMOR_BAM = "tumor.bam";
    private static final String REF_BAM = "ref.baf";
    private OutputDataset victim;
    private ArgumentCaptor<byte[]> datasetBytes;

    @Before
    public void setUp() throws Exception {
        Bucket reportBucket = mock(Bucket.class);
        victim = new OutputDataset(reportBucket, TestInputs.SET);
        datasetBytes = ArgumentCaptor.forClass(byte[].class);
        Blob datasetBlob = TestBlobs.blob(OutputDataset.DATASET_JSON);
        when(reportBucket.create(eq(TestInputs.SET + "/" + OutputDataset.DATASET_JSON), datasetBytes.capture())).thenReturn(datasetBlob);
    }

    @Test
    public void emptyDatasetReturnsEmptyJson() {
        victim.serializeAndUpload();
        String emptyJson = new String(datasetBytes.getValue());
        assertThat(emptyJson).isEqualTo("{}");
    }

    @Test
    public void createDatasetJsonIncludingAddedDatatypesForSomaticForFile() throws Exception {
        final String amberPathFromRoot = "amber/" + AMBER_BAF;
        Blob amberBlob = TestBlobs.blob(TestInputs.SET + "/" + amberPathFromRoot);
        victim.add(new AddDatatype(DataType.AMBER, TestInputs.tumorSample(), new ArchivePath(Folder.root(), Amber.NAMESPACE, AMBER_BAF)),
                amberBlob);
        victim.serializeAndUpload();
        Dataset dataset = ObjectMappers.get().readValue(datasetBytes.getValue(), Dataset.class);
        assertThatDatatypeIs(dataset.getAmber(), amberPathFromRoot, TestInputs.tumorSample(), false);
    }

    @Test
    public void createDatasetJsonIncludingAddedDatatypesForSomaticForDirectory() throws Exception {
        final String amberPathFromRoot = "amber/" + AMBER_BAF;
        Blob amberBlob = TestBlobs.blob(TestInputs.SET + "/" + amberPathFromRoot);
        victim.add(new AddDatatype(DataType.AMBER, TestInputs.tumorSample(), new ArchivePath(Folder.root(), Amber.NAMESPACE, AMBER_BAF), true),
                amberBlob);
        victim.serializeAndUpload();
        Dataset dataset = ObjectMappers.get().readValue(datasetBytes.getValue(), Dataset.class);
        assertThatDatatypeIs(dataset.getAmber(), amberPathFromRoot, TestInputs.tumorSample(), true);
    }

    @Test
    public void createDatasetJsonIncludingAddedDatatypesForSingleSample() throws Exception {
        final String tumorBamPathFromRoot = TestInputs.tumorSample() + "/aligner/" + TUMOR_BAM;
        Blob tumorBam = TestBlobs.blob(TestInputs.SET + "/" + tumorBamPathFromRoot);
        final String refBamPathFromRoot = TestInputs.referenceSample() + "/aligner/" + REF_BAM;
        Blob refBam = TestBlobs.blob(TestInputs.SET + "/" + refBamPathFromRoot);
        victim.add(new AddDatatype(DataType.ALIGNED_READS,
                TestInputs.tumorSample(),
                new ArchivePath(Folder.from(TestInputs.tumorRunMetadata()), Aligner.NAMESPACE, TUMOR_BAM)), tumorBam);
        victim.add(new AddDatatype(DataType.ALIGNED_READS,
                TestInputs.referenceSample(),
                new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), Aligner.NAMESPACE, REF_BAM)), refBam);
        victim.serializeAndUpload();
        Dataset dataset = ObjectMappers.get().readValue(datasetBytes.getValue(), Dataset.class);
        assertThatDatatypeIs(dataset.getAlignedReads(), tumorBamPathFromRoot, TestInputs.tumorSample(), false);
        assertThatDatatypeIs(dataset.getAlignedReads(), refBamPathFromRoot, TestInputs.referenceSample(), false);
    }

    private void assertThatDatatypeIs(final Map<String, DatasetFile> datasetFileMap, final String fileName, final String sample,
            final boolean isDirectory) {
        assertThat(datasetFileMap).isNotNull();
        final DatasetFile datasetFile = datasetFileMap.get(sample);
        assertThat(datasetFile).isNotNull();
        assertThat(datasetFile.getPath()).isEqualTo(fileName);
        assertThat(datasetFile.getIsDirectory()).isEqualTo(isDirectory);
    }
}