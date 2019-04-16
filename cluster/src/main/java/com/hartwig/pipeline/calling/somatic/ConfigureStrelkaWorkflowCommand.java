package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class ConfigureStrelkaWorkflowCommand implements BashCommand {

    private final String tumorPath;
    private final String referencePath;
    private final String configPath;
    private final String referenceGenomePath;

    ConfigureStrelkaWorkflowCommand(final String tumorPath, final String referencePath, final String configPath, final String referenceGenomePath) {
        this.tumorPath = tumorPath;
        this.referencePath = referencePath;
        this.configPath = configPath;
        this.referenceGenomePath = referenceGenomePath;
    }

    @Override
    public String asBash() {
        return String.format("/data/tools/strelka_v1.0.14/bin/configureStrelkaWorkflow.pl --tumor %s --normal %s --config %s --ref %s",
                tumorPath,
                referencePath,
                configPath,
                referenceGenomePath);
    }
}
