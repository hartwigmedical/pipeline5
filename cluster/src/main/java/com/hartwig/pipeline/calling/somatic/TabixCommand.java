package com.hartwig.pipeline.calling.somatic;

class TabixCommand extends VersionedToolCommand{

    TabixCommand(String vcf) {
        super("tabix","tabix", "0.2.6", vcf, "-p", "vcf");
    }
}
