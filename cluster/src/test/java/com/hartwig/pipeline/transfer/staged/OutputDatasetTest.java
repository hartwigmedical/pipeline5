package com.hartwig.pipeline.transfer.staged;

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
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;

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
        victim.serialize();
        String emptyJson = new String(datasetBytes.getValue());
        assertThat(emptyJson).isEqualTo("{}");
    }

    @Test
    public void createDatasetJsonIncludingAddedDatatypesForSomatic() throws Exception {
        Blob amberBlob = TestBlobs.blob(AMBER_BAF);
        victim.add(new AddDatatype(DataType.AMBER, TestInputs.tumorSample(), new ArchivePath(Folder.root(), Amber.NAMESPACE, AMBER_BAF)),
                amberBlob);
        victim.serialize();
        Dataset dataset = ObjectMappers.get().readValue(datasetBytes.getValue(), Dataset.class);
        assertThatDatatypeIs(dataset.getAmber(), AMBER_BAF, TestInputs.tumorSample());
    }

    @Test
    public void createDatasetJsonIncludingAddedDatatypesForSingleSample() throws Exception {
        Blob tumorBam = TestBlobs.blob(TUMOR_BAM);
        Blob refBam = TestBlobs.blob(REF_BAM);
        victim.add(new AddDatatype(DataType.ALIGNED_READS,
                TestInputs.tumorSample(),
                new ArchivePath(Folder.from(TestInputs.tumorRunMetadata()), Aligner.NAMESPACE, TUMOR_BAM)), tumorBam);
        victim.add(new AddDatatype(DataType.ALIGNED_READS,
                TestInputs.referenceSample(),
                new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), Aligner.NAMESPACE, REF_BAM)), refBam);
        victim.serialize();
        Dataset dataset = ObjectMappers.get().readValue(datasetBytes.getValue(), Dataset.class);
        assertThatDatatypeIs(dataset.getAlignedReads(), TUMOR_BAM, TestInputs.tumorSample());
        assertThatDatatypeIs(dataset.getAlignedReads(), REF_BAM, TestInputs.referenceSample());
    }

    private void assertThatDatatypeIs(final Map<String, DatasetFile> alignedReads, final String fileName, final String sample) {
        assertThat(alignedReads).isNotNull();
        final DatasetFile tumorBamFile = alignedReads.get(sample);
        assertThat(tumorBamFile).isNotNull();
        assertThat(tumorBamFile.getPath()).isEqualTo(fileName);
    }
}