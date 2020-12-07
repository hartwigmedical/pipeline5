package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.tools.Versions;

import org.junit.Test;

public class GridssAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new GridssAnnotation(TestInputs.HG19_RESOURCE_FILES, true);
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gridss.unfiltered.vcf.gz";
    }

    @Test
    public void expectedOutput() {
        final String expectedGridssCall =
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/"
                        + Versions.GRIDSS + "/gridss.jar";
        assertThat(output.bash().get(0).asBash()).contains(expectedGridssCall
                + " gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta INPUT=/data/output/tumor.strelka.vcf OUTPUT=/data/output/tumor.strelka.vcf.repeatmasker.vcf.gz ALIGNMENT=REPLACE WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo) REPEAT_MASKER_BED=/opt/resources/gridss_repeatmasker_db/hg19/hg19.fa.out.bed");
        assertThat(output.bash().get(1).asBash()).contains(expectedGridssCall
                + " gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/tumor.strelka.vcf.repeatmasker.vcf.gz OUTPUT=/data/output/tumor.gridss.unfiltered.vcf.gz ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo)");
    }

}
