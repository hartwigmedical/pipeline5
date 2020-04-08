package com.hartwig.pipeline.resource;

public enum RefGenomeVersion {
    HG37,
    HG38;

    public static int asInt(RefGenomeVersion version)
    {
        return version == HG37 ? 37 : 38;
    }

    public static String asString(RefGenomeVersion version)
    {
        return version == HG37 ? "37" : "38";
    }
}
