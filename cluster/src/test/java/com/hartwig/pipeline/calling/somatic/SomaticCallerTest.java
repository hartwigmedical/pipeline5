package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;

import org.jetbrains.annotations.NotNull;
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
        mockResource(storage, "reference_genome", "reference.fasta");
        mockResource(storage, "sage-pilot", "hotspots.tsv", "coding_regions.bed", "SAGE_PON.vcf.gz");
        mockResource(storage, "strelka_config", "strelka.ini");
        mockResource(storage, "hg19_mappability_tracks", "mappability.bed.gz", "mappability.hdr");
        mockResource(storage, "pon-v2", "GERMLINE_PON.vcf.gz", "SOMATIC_PON.vcf.gz");
        mockResource(storage, "beds", "strelka-post-process.bed");
        mockResource(storage, "snpeff", "snpeff.config");
        mockResource(storage, "known_snps", "dbsnp.vcf.gz");
        mockResource(storage, "cosmic_v85", "cosmic.vcf.gz");
        computeEngine = mock(ComputeEngine.class);
        victim = new SomaticCaller(Arguments.testDefaults(), computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsFinalVcfGoogleStorageLocation() {
        AlignmentPair input = createInput();
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        assertThat(victim.run(input)).isEqualTo(SomaticCallerOutput.builder()
                .status(JobStatus.SUCCESS)
                .finalSomaticVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "results/data/output/tumor.cosmic.annotated.vcf.gz"))
                .build());
    }

    @Test
    public void downloadsRecalibratedBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinition = ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinition.capture())).thenReturn(JobStatus.SUCCESS);
        victim.run(createInput());
        assertThat(jobDefinition.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-tumor/tumor.recalibrated.bam /data/input/tumor.recalibrated.bam",
                "gsutil -qm cp gs://run-reference/reference.recalibrated.bam /data/input/reference.recalibrated.bam",
                "gsutil -qm cp gs://run-tumor/tumor.recalibrated.bam.bai /data/input/tumor.recalibrated.bam.bai",
                "gsutil -qm cp gs://run-reference/reference.recalibrated.bam.bai /data/input/reference.recalibrated.bam.bai");
    }

    @Test
    public void returnsFailedStatusWhenJobFails() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(victim.run(createInput()).status()).isEqualTo(JobStatus.FAILED);
    }

    private void mockResource(final Storage storage, final String resourceName, final String... fileNames) {
        Bucket referenceGenomeBucket = mock(Bucket.class);
        List<Blob> blobs = new ArrayList<>();
        for (String fileName : fileNames) {
            Blob blob = mock(Blob.class);
            when(blob.getName()).thenReturn(fileName);
            blobs.add(blob);
        }
        @SuppressWarnings("unchecked")
        Page<Blob> page = mock(Page.class);
        when(page.iterateAll()).thenReturn(blobs);
        when(storage.get(resourceName)).thenReturn(referenceGenomeBucket);
        when(referenceGenomeBucket.list()).thenReturn(page);
    }

    private AlignmentPair createInput() {
        return AlignmentPair.of(alignerOutput("reference"), alignerOutput("tumor"));
    }

    @NotNull
    private AlignmentOutput alignerOutput(final String sample) {
        String bucket = "run-" + sample;
        return AlignmentOutput.of(gsLocation(bucket, sample + ".bam"),
                gsLocation(bucket, sample + ".bam.bai"),
                gsLocation(bucket, sample + ".recalibrated.bam"),
                gsLocation(bucket, sample + ".recalibrated.bam.bai"),
                Sample.builder("", sample).build());
    }

    @NotNull
    private GoogleStorageLocation gsLocation(final String bucket, final String path) {
        return GoogleStorageLocation.of(bucket, path);
    }

}