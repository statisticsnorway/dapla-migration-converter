package no.ssb.rawdata.converter.app.migration;

import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import no.ssb.rawdata.converter.app.migration.pubsub.PubSubJobRequestClient;
import no.ssb.rawdata.converter.core.datasetmeta.DatasetType;
import no.ssb.rawdata.converter.core.datasetmeta.Valuation;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;

@MicronautTest
public class RawdataConverterAppTest {

    @Inject
    EmbeddedApplication application;

    @Inject
    PubSubJobRequestClient pubSubJobRequestClient;

    @Test
    void testItWorks() throws InterruptedException {
        Assertions.assertTrue(application.isRunning());

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

        pubSubJobRequestClient.send(jobConfig);

        System.out.printf("SENT JOB WITH ID: %s%n", jobConfig.getJobId());

        Thread.sleep(1000);
    }

}
