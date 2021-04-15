package no.ssb.rawdata.converter.app.migration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.dlp.pseudo.core.PseudoFuncRule;
import no.ssb.rawdata.converter.core.datasetmeta.DatasetType;
import no.ssb.rawdata.converter.core.datasetmeta.Valuation;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigDeserializeWorkaround;
import no.ssb.rawdata.converter.util.Json;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JobConfigTest {

    @Test
    public void jobToJson() throws JsonProcessingException {
        ConverterJobConfig jobConfig = new ConverterJobConfig("test-job")
                .setJobId("test-id-" + UUID.randomUUID())
                .setPrototype(false)
                .setParent("base")
                .setActiveByDefault(true);
        jobConfig.getDebug()
                .setDryrun(false)
                .setDevelopmentMode(true)
                .setLogFailedRawdata(true)
                .setStoreFailedRawdata(false)
                .setLocalStoragePath("./rawdata-messages-output")
                .setIncludedRawdataEntries(new LinkedHashSet<>(List.of("entry1", "entry2")));
        jobConfig.getConverterSettings()
                .setSkippedMessages(new LinkedHashSet<>(List.of("ulid-A", "ulid-B")))
                .setMaxRecordsBeforeFlush(1000L)
                .setMaxSecondsBeforeFlush(60L);
        jobConfig.getTargetDataset()
                .setType(DatasetType.BOUNDED)
                .setValuation(Valuation.INTERNAL)
                .setPublishMetadata(false);
        jobConfig.getPseudoRules().add(new PseudoFuncRule("my-fnr-rule", "**/Fodselsnummer", "fpe-fnr(somesecret)"));
        jobConfig.getPseudoRules().add(new PseudoFuncRule("my-navn-rule", "**/Navn", "fpe-anychar(somesecret)"));
        jobConfig.getRawdataSource()
                .setEncryptionKeyId("integration-test-encryption-key-id")
                .setEncryptionKeyVersion("1")
                .setEncryptionSalt("encryption-salt".getBytes(StandardCharsets.UTF_8))
                .setEncryptionKey("key".toCharArray())
                .setName("somerawdata")
                .setTopic("persontull")
                .setInitialPosition("FIRST");
        jobConfig.getTargetStorage()
                .setPath("/kilde/test/persontull")
                .setVersion("" + System.currentTimeMillis());

        ObjectMapper mapper = new ObjectMapper();
        String jobJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jobConfig);
        System.out.printf("%s%n", jobJson);

        ConverterJobConfig copiedJob = mapper.readValue(jobJson, ConverterJobConfigDeserializeWorkaround.class);
        String copiedJobJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(copiedJob);

        assertEquals(jobJson, copiedJobJson);
        assertEquals(jobConfig, copiedJob);
    }

    @Test
    public void deserializeCase1() {
        String json = """
                {
                  "jobId" : "c65b4d6f-4133-4039-a0b1-eeabb7f98dee",
                  "jobName" : "csv migration of ssb/stamme01/statunit/domain/b/persontull.csv",
                  "prototype" : false,
                  "parent" : "base",
                  "activeByDefault" : "true",
                  "debug" : {
                    "dryrun" : false,
                    "developmentMode" : true,
                    "logFailedRawdata" : true,
                    "logSkippedRawdata" : null,
                    "logAllRawdata" : null,
                    "logAllConverted" : null,
                    "storeFailedRawdata" : false,
                    "storeSkippedRawdata" : null,
                    "storeAllRawdata" : null,
                    "storeAllConverted" : null,
                    "localStoragePath" : null,
                    "localStoragePassword" : null,
                    "includedRawdataEntries" : null
                  },
                  "converterSettings" : {
                    "maxRecordsBeforeFlush" : null,
                    "maxSecondsBeforeFlush" : null,
                    "maxRecordsTotal" : null,
                    "rawdataSamples" : null,
                    "skippedMessages" : null
                  },
                  "rawdataSource" : {
                    "name" : null,
                    "topic" : "persontull",
                    "initialPosition" : "LAST",
                    "encryptionKey" : null,
                    "encryptionKeyId" : "my-secret-key",
                    "encryptionKeyVersion" : "1",
                    "encryptionSalt" : "ZW5jcnlwdGlvbi1zYWx0"
                  },
                  "targetStorage" : {
                    "root" : null,
                    "path" : "kilde/persontull",
                    "version" : "1616075144530",
                    "saKeyFile" : null
                  },
                  "targetDataset" : {
                    "valuation" : "INTERNAL",
                    "type" : "BOUNDED",
                    "publishMetadata" : false
                  },
                  "appConfig" : { },
                  "pseudoRules" : [ ]
                }""";
        ConverterJobConfig converterJobConfig = Json.toObject(ConverterJobConfigDeserializeWorkaround.class, json);
        converterJobConfig.toString();
    }
}
