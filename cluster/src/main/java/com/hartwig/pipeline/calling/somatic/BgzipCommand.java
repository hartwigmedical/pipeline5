package com.hartwig.pipeline.calling.somatic;

class BgzipCommand extends VersionedToolCommand {

    BgzipCommand(String vcf) {
        super("tabix", "bgzip", "0.2.6", "-f", vcf);
    }
}
