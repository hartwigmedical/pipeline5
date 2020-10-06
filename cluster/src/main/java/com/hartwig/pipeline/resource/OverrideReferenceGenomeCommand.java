package com.hartwig.pipeline.resource;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class OverrideReferenceGenomeCommand {

    static List<BashCommand> overrides(final ResourceFiles resourceFiles) {
        if(resourceFiles.refGenomeFile().startsWith("gs://")){
            return Collections.singletonList(new BashCommand() {
                @Override
                public String asBash() {
                    String fastaFilename = "";
                    return "gsutil -qm cp -n " + resourceFiles.refGenomeFile() + "* /opt/resources/";
                }
            })
        }
    }
}
