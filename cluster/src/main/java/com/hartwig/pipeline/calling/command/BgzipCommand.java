package com.hartwig.pipeline.calling.command;

public class BgzipCommand extends VersionedToolCommand {

    public BgzipCommand(String vcf) {
        super("tabix", "bgzip", "0.2.6", "-f", vcf);
    }
}
