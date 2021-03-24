package no.ssb.rawdata.converter.app.migration.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.micronaut.context.annotation.Context;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.migration.protocol.Command;
import no.ssb.dapla.migration.protocol.CommandContext;
import no.ssb.dapla.migration.protocol.Status;
import no.ssb.dapla.migration.protocol.Target;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.util.Json;

import javax.inject.Singleton;
import java.time.ZonedDateTime;

@Singleton
@Context
@Slf4j
public class PubSubResponseService {

    private final PubSubJobResponseClient pubSubJobResponseClient;

    public PubSubResponseService(PubSubJobResponseClient pubSubJobResponseClient) {
        this.pubSubJobResponseClient = pubSubJobResponseClient;
    }

    public void sendInProgress(ConverterJobConfig jobConfig, ZonedDateTime startTime, long messagesConverted) {
        JsonNode jobConfigNode = Json.toObject(JsonNode.class, Json.from(jobConfig));
        CommandContext commandContext = CommandContext.builder()
                .id(jobConfig.getJobId())
                .cmd(Command.CONVERT)
                .status(Status.IN_PROGRESS)
                .target(Target.CONVERTER)
                .startTime(startTime)
                .timestamp(ZonedDateTime.now())
                .arg("jobConfig", jobConfigNode)
                .addResult("messagesConverted", String.valueOf(messagesConverted))
                .build();
        pubSubJobResponseClient.send(PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(commandContext.toPrettyJson()))
                .build());
    }

    public void sendComplete(ConverterJobConfig jobConfig, ZonedDateTime startTime, long messagesConverted) {
        JsonNode jobConfigNode = Json.toObject(JsonNode.class, Json.from(jobConfig));
        CommandContext commandContext = CommandContext.builder()
                .id(jobConfig.getJobId())
                .cmd(Command.CONVERT)
                .status(Status.COMPLETED)
                .target(Target.CONVERTER)
                .startTime(startTime)
                .timestamp(ZonedDateTime.now())
                .arg("jobConfig", jobConfigNode)
                .addResult("messagesConverted", String.valueOf(messagesConverted))
                .build();
        pubSubJobResponseClient.send(PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(commandContext.toPrettyJson()))
                .build());
    }

    public void sendError(ConverterJobConfig jobConfig, ZonedDateTime startTime, long messagesConverted, Throwable throwable) {
        JsonNode jobConfigNode = Json.toObject(JsonNode.class, Json.from(jobConfig));
        CommandContext commandContext = CommandContext.builder()
                .id(jobConfig.getJobId())
                .cmd(Command.CONVERT)
                .status(Status.ERROR)
                .target(Target.CONVERTER)
                .errorCause(throwable.getMessage())
                .startTime(startTime)
                .timestamp(ZonedDateTime.now())
                .arg("jobConfig", jobConfigNode)
                .addResult("messagesConverted", String.valueOf(messagesConverted))
                .build();
        pubSubJobResponseClient.send(PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(commandContext.toPrettyJson()))
                .build());
    }
}
