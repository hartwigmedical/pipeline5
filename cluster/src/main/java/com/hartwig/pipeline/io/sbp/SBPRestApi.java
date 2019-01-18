package com.hartwig.pipeline.io.sbp;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPRestApi {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(SBPRestApi.class);
    private final WebTarget target;

    private SBPRestApi(final WebTarget target) {
        this.target = target;
    }

    String getFastQ(int sampleId) {
        return "[\n" + "  {\n" + "    \"qc_pass\": true,\n" + "    \"q30\": 55.9669,\n" + "    \"sample_id\": 64,\n" + "    \"id\": 91,\n"
                + "    \"name_r1\": \"CPCT12345678R_HJJLGCCXX_S1_L001_R1_001.fastq.gz\",\n"
                + "    \"name_r2\": \"CPCT12345678R_HJJLGCCXX_S1_L001_R2_001.fastq.gz\",\n" + "    \"lane_id\": 58,\n"
                + "    \"bucket\": \"hmf-fastq-storage\",\n" + "    \"yld\": 59702984,\n" + "    \"hash_r2\": null,\n"
                + "    \"hash_r1\": null,\n" + "    \"size_r2\": null,\n" + "    \"size_r1\": null\n" + "  }]";
    }

    @NotNull
    private RuntimeException error(final Response response) {
        return new RuntimeException(String.format("Received an error status of [%s] from SBP Api at [%s]",
                response.getStatus(),
                target.getUri()));
    }

    String getSample(int sampleId) {
        return "{\n" + "  \"status\": \"Deleted\",\n" + "  \"updateTime\": \"2017-07-25T18:05:57\",\n" + "  \"yld_req\": 25000000000,\n"
                + "  \"hash\": null,\n" + "  \"name\": \"ZR17SQ1-00649\",\n" + "  \"submission\": \"HMFreg0147\",\n"
                + "  \"barcode\": \"FR13257296\",\n" + "  \"bucket\": null,\n" + "  \"createTime\": \"2017-06-25T23:10:55\",\n"
                + "  \"yld\": 34589708618,\n" + "  \"q30\": 67.878,\n" + "  \"filesize\": null,\n" + "  \"directory\": null,\n"
                + "  \"filename\": null,\n" + "  \"type\": \"ref\",\n" + "  \"id\": 49,\n" + "  \"q30_req\": 75.0\n" + "}";
    }

    private WebTarget samplesApi() {
        return target.path("hmf").path("v1").path("samples");
    }

    void patchBam(int sampleId, BamMetadata metadata) {
        // do noting
    }

    public static SBPRestApi newInstance(Arguments arguments) {
        ClientConfig config = new ClientConfig();
        config.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
        return new SBPRestApi(ClientBuilder.newBuilder().withConfig(config).build().target(arguments.sbpApiUrl()));
    }
}
