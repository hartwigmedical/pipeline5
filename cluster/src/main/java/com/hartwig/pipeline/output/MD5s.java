package com.hartwig.pipeline.output;

import java.util.Base64;

import org.apache.commons.codec.binary.Hex;

public class MD5s {

    public static String asHex(final String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }
}
