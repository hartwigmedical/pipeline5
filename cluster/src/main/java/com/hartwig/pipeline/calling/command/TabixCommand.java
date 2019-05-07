package com.hartwig.pipeline.calling.command;

public class TabixCommand extends VersionedToolCommand {

    public TabixCommand(String vcf) {
        super("tabix","tabix", "0.2.6", vcf, "-p", "vcf");
    }
}
