package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.Collections;

public class SortVcf extends GridssCommand {

    public SortVcf(final String withBealn, final String annotatedBealn, final String outputPath) {
        super("picard.cmdline.PicardCommandLine SortVcf",
                "32G",
                Collections.emptyList(),
                "I=" + withBealn,
                "I=" + annotatedBealn,
                "O=" + outputPath);
    }
}
