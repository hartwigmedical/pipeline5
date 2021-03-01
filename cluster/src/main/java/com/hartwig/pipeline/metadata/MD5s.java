package com.hartwig.pipeline.metadata;

import java.util.Base64;

import org.apache.commons.codec.binary.Hex;

public class MD5s {

    static String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }
}
