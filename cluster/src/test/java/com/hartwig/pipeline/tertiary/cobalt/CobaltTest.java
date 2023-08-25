package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.computeengine.input.SomaticRunMetadata;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.tools.HmfTool;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static org.assertj.core.api.Assertions.assertThat;

public class CobaltTest extends TertiaryStageTest<CobaltOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<CobaltOutput, SomaticRunMetadata> createVictim() {
        return new Cobalt(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset, Arguments.testDefaults());
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                toolCommand(HmfTool.COBALT)
                        + " -tumor tumor -tumor_bam /data/input/tumor.bam "
                        + "-reference reference -reference_bam /data/input/reference.bam "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-gc_profile /opt/resources/gc_profiles/37/GC_profile.1000bp.37.cnp "
                        + "-output_dir /data/output "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    protected void validateOutput(final CobaltOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo(expectedRuntimeBucketName() + "/" + Cobalt.NAMESPACE);
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }

    @Override
    protected void validatePersistedOutput(final CobaltOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/cobalt", true));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.COBALT,
                TestInputs.defaultSomaticRunMetadata().barcode(),
                new ArchivePath(Folder.root(), Cobalt.NAMESPACE, "tumor.cobalt.ratio.tsv.gz"), true));
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final CobaltOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "cobalt", true));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.COBALT, "cobalt");
    }
}