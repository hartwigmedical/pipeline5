package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.COSMIC;
import static com.hartwig.pipeline.resource.ResourceNames.DBSNPS;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PON;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;
import static com.hartwig.pipeline.resource.ResourceNames.STRELKA_CONFIG;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        MockResource.addToStorage(storage, REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, SAGE, "hotspots.tsv", "coding_regions.bed", "SAGE_PON.vcf.gz");
        MockResource.addToStorage(storage, STRELKA_CONFIG, "strelka.ini");
        MockResource.addToStorage(storage, MAPPABILITY, "mappability.bed.gz", "mappability.hdr");
        MockResource.addToStorage(storage, PON, "GERMLINE_PON.vcf.gz", "SOMATIC_PON.vcf.gz");
        MockResource.addToStorage(storage, BEDS, "strelka-post-process.bed");
        MockResource.addToStorage(storage, SNPEFF, "snpeff.config");
        MockResource.addToStorage(storage, DBSNPS, "dbsnp.vcf.gz");
        MockResource.addToStorage(storage, COSMIC, "cosmic.vcf.gz");
        computeEngine = mock(ComputeEngine.class);
        victim = new SomaticCaller(Arguments.testDefaults(), computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedIfDisabledInArguments() {
        assertThat(new SomaticCaller(Arguments.testDefaultsBuilder().runSomaticCaller(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory()).run(TestInputs.defaultPair()).status()).isEqualTo(JobStatus.SKIPPED);
    }

    @Test
    public void returnsFinalVcfGoogleStorageLocation() {
        AlignmentPair input = TestInputs.defaultPair();
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        assertThat(victim.run(input).finalSomaticVcf()).isEqualTo(GoogleStorageLocation.of(RUNTIME_BUCKET + "/" + SomaticCaller.NAMESPACE,
                "results/tumor.cosmic.annotated.vcf.gz"));
    }

    @Test
    public void downloadsRecalibratedBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinition = ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinition.capture())).thenReturn(JobStatus.SUCCESS);
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinition.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-tumor/aligner/results/tumor.bam /data/input/tumor.bam",
                "gsutil -qm cp gs://run-reference/aligner/results/reference.bam /data/input/reference.bam",
                "gsutil -qm cp gs://run-tumor/aligner/results/tumor.bam.bai /data/input/tumor.bam.bai",
                "gsutil -qm cp gs://run-reference/aligner/results/reference.bam.bai /data/input/reference.bam.bai");
    }

    @Test
    public void returnsFailedStatusWhenJobFails() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(victim.run(TestInputs.defaultPair()).status()).isEqualTo(JobStatus.FAILED);
    }
}