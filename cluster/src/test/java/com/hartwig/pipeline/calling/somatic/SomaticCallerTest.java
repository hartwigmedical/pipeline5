package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SomaticCallerTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor";
    private ComputeEngine computeEngine;
    private SomaticCaller victim;

    @Before
    public void setUp() throws Exception {
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        MockResource.addToStorage(storage, "reference_genome", "reference.fasta");
        MockResource.addToStorage(storage, "sage-pilot", "hotspots.tsv", "coding_regions.bed", "SAGE_PON.vcf.gz");
        MockResource.addToStorage(storage, "strelka_config", "strelka.ini");
        MockResource.addToStorage(storage, "hg19_mappability_tracks", "mappability.bed.gz", "mappability.hdr");
        MockResource.addToStorage(storage, "pon-v2", "GERMLINE_PON.vcf.gz", "SOMATIC_PON.vcf.gz");
        MockResource.addToStorage(storage, "beds", "strelka-post-process.bed");
        MockResource.addToStorage(storage, "snpeff", "snpeff.config");
        MockResource.addToStorage(storage, "known_snps", "dbsnp.vcf.gz");
        MockResource.addToStorage(storage, "cosmic_v85", "cosmic.vcf.gz");
        computeEngine = mock(ComputeEngine.class);
        victim = new SomaticCaller(Arguments.testDefaults(), computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsFinalVcfGoogleStorageLocation() {
        AlignmentPair input = TestInputs.defaultPair();
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        assertThat(victim.run(input)).isEqualTo(SomaticCallerOutput.builder()
                .status(JobStatus.SUCCESS)
                .finalSomaticVcf(GoogleStorageLocation.of(RUNTIME_BUCKET + "/" + SomaticCaller.NAMESPACE,
                        "results/tumor.cosmic.annotated.vcf.gz"))
                .build());
    }

    @Test
    public void downloadsRecalibratedBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinition = ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinition.capture())).thenReturn(JobStatus.SUCCESS);
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinition.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-tumor/aligner/results/tumor.recalibrated.bam /data/input/tumor.recalibrated.bam",
                "gsutil -qm cp gs://run-reference/aligner/results/reference.recalibrated.bam /data/input/reference.recalibrated.bam",
                "gsutil -qm cp gs://run-tumor/aligner/results/tumor.recalibrated.bam.bai /data/input/tumor.recalibrated.bam.bai",
                "gsutil -qm cp gs://run-reference/aligner/results/reference.recalibrated.bam.bai /data/input/reference.recalibrated.bam.bai");
    }

    @Test
    public void returnsFailedStatusWhenJobFails() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(victim.run(TestInputs.defaultPair()).status()).isEqualTo(JobStatus.FAILED);
    }
}