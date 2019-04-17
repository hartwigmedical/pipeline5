package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashCommand;

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
        return String.format("/data/tools/strelka_v1.0.14/bin/configureStrelkaWorkflow.pl --tumor %s "
                        + "--normal %s --config %s --ref %s --output-dir %s",
                tumorPath,
                referencePath,
                configPath,
                referenceGenomePath,
                outputDirectory);
    }
}
