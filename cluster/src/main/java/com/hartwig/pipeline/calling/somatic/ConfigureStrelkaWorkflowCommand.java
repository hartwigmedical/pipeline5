package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.tools.Versions;

public class ConfigureStrelkaWorkflowCommand implements BashCommand {

    private final String tumorPath;
    private final String referencePath;
    private final String configPath;
    private final String referenceGenomePath;
    private final String outputDirectory;

    ConfigureStrelkaWorkflowCommand(final String tumorPath, final String referencePath, final String configPath,
            final String referenceGenomePath, final String outputDirectory) {
        this.tumorPath = tumorPath;
        this.referencePath = referencePath;
        this.configPath = configPath;
        this.referenceGenomePath = referenceGenomePath;
        this.outputDirectory = outputDirectory;
    }

    @Override
    public String asBash() {
        return String.format(
                "/opt/tools/strelka/%s/bin/configureStrelkaWorkflow.pl --tumor %s " + "--normal %s --config %s --ref %s --output-dir %s",
                Versions.STRELKA,
                tumorPath,
                referencePath,
                configPath,
                referenceGenomePath,
                outputDirectory);
    }
}
