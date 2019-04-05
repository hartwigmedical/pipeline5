package com.hartwig.pipeline.resource;

import java.util.function.Function;

public class ReferenceGenomeAlias implements Function<String, String> {

    @Override
    public String apply(final String s) {
        String extension = findExtension(s);
        return alias(extension, s);
    }

    private static String findExtension(final String s) {
        if (s.contains("fasta")) {
            return "fasta";
        } else if (s.endsWith("fa") || s.contains("fa.")) {
            return "fa";
        } else {
            throw new IllegalArgumentException(String.format("Can only alias reference fasta files with extensions fa or fasta. "
                    + "Please remove [%s] from the reference genome bucket or rename appropriately", s));
        }
    }

    private String alias(final String extension, final String name) {
        return "reference." + name.substring(name.indexOf(extension), name.length());
    }
}
