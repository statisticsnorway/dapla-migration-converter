package no.ssb.rawdata.converter.app.migration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.rawdata.converter.core.datasetmeta.DatasetType;
import no.ssb.rawdata.converter.core.datasetmeta.Valuation;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JobConfigTest {

    @Test
    public void jobToJson() throws JsonProcessingException {
        ConverterJobConfig jobConfig = new ConverterJobConfig("test-job")
                .setPrototype(false)
                .setParent("base")
                .setActiveByDefault(false);
        jobConfig.getDebug()
                .setDryrun(true)
                .setDevelopmentMode(true)
                .setLogFailedRawdata(true)
                .setStoreFailedRawdata(false)
                .setLocalStoragePath("./rawdata-messages-output");
        jobConfig.getConverterSettings()
                .setMaxRecordsBeforeFlush(1000L)
                .setMaxSecondsBeforeFlush(60L);
        jobConfig.getTargetDataset()
                .setType(DatasetType.BOUNDED)
                .setValuation(Valuation.INTERNAL)
                .setPublishMetadata(true);
        jobConfig.getPseudoRules().clear();
        jobConfig.getRawdataSource()
                .setEncryptionKeyId("encryption-key-id")
                .setEncryptionKeyVersion("1")
                .setEncryptionSalt("encryption-salt".getBytes(StandardCharsets.UTF_8))
                .setName("The name?")
                .setTopic("the-topic")
                .setInitialPosition("FIRST");
        jobConfig.getTargetStorage()
                .setPath("/kilde/stamme01/foo/bar/dataset-navn")
                .setVersion("" + System.currentTimeMillis());

        ObjectMapper mapper = new ObjectMapper();
        String jobJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jobConfig);
        System.out.printf("%s%n", jobJson);

        ConverterJobConfig copiedJob = mapper.readValue(jobJson, ConverterJobConfig.class);
        String copiedJobJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(copiedJob);
        System.out.printf("%s%n", copiedJobJson);

        assertEquals(jobJson, copiedJobJson);
        assertEquals(jobConfig, copiedJob);
    }
}
