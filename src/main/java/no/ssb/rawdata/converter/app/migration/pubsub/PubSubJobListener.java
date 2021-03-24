package no.ssb.rawdata.converter.app.migration.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.pubsub.v1.PubsubMessage;
import io.micronaut.gcp.pubsub.annotation.PubSubListener;
import io.micronaut.gcp.pubsub.annotation.Subscription;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.migration.protocol.Command;
import no.ssb.dapla.migration.protocol.CommandContext;
import no.ssb.dapla.migration.protocol.Status;
import no.ssb.dapla.migration.protocol.Target;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigDeserializeWorkaround;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigFactory;
import no.ssb.rawdata.converter.core.job.ConverterJobScheduler;
import no.ssb.rawdata.converter.util.Json;

@PubSubListener
@Slf4j
public class PubSubJobListener {

    final ConverterJobScheduler jobScheduler;
    final ConverterJobConfigFactory jobConfigFactory;

    public PubSubJobListener(ConverterJobScheduler jobScheduler, ConverterJobConfigFactory jobConfigFactory) {
        this.jobScheduler = jobScheduler;
        this.jobConfigFactory = jobConfigFactory;
    }

    @Subscription("migration-converter-job-requests")
    public void onMessage(PubsubMessage message) {
        try {
            String json = message.getData().toStringUtf8();
            System.out.printf("Job Received: %s%n", json);
            CommandContext commandContext = CommandContext.of(json).build();
            if (commandContext.target() != Target.CONVERTER) {
                log.warn("Received CommandContext with target other than CONVERTER");
                return;
            }
            if (commandContext.cmd() != Command.CONVERT) {
                log.warn("Received CommandContext with cmd other than CONVERT");
                return;
            }
            if (commandContext.status() != Status.CREATED) {
                log.warn("Received CommandContext with status other than CREATED");
                return;
            }
            JsonNode jobConfigNode = commandContext.arg("jobConfig");
            ConverterJobConfig converterJobConfig = Json.toObject(ConverterJobConfigDeserializeWorkaround.class, jobConfigNode.toString());
            System.out.printf("Received JOB-ID: %s%n", converterJobConfig.getJobId());

            jobScheduler.schedulePartial(converterJobConfig);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
