package no.ssb.rawdata.converter.app.migration.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import io.micronaut.gcp.pubsub.annotation.PubSubListener;
import io.micronaut.gcp.pubsub.annotation.Subscription;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigDeserializeWorkaround;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigFactory;
import no.ssb.rawdata.converter.core.job.ConverterJobScheduler;
import no.ssb.rawdata.converter.util.Json;

@PubSubListener
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
            System.out.printf("ConverterJob Received: %s%n", json);
            ConverterJobConfig converterJobConfig = Json.toObject(ConverterJobConfigDeserializeWorkaround.class, json);
            System.out.printf("Received JOB-ID: %s%n", converterJobConfig.getJobId());

            jobScheduler.schedulePartial(converterJobConfig);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
