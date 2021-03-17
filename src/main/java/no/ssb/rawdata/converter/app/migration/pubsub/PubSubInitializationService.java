package no.ssb.rawdata.converter.app.migration.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;

@Singleton
@Context
@Slf4j
public class PubSubInitializationService {

    final PubSubJobResponseClient client;
    final TransportChannelProvider transportChannelProvider;
    final CredentialsProvider credentialsProvider;
    private final PubSubResponseConfig pubSubResponseConfig;
    private final PubSubRequestConfig pubSubRequestConfig;
    final String projectId;

    public PubSubInitializationService(PubSubJobResponseClient client,
                                       @Named("pubsub") TransportChannelProvider transportChannelProvider,
                                       @Named("pubsub") CredentialsProvider credentialsProvider,
                                       PubSubResponseConfig pubSubResponseConfig,
                                       PubSubRequestConfig pubSubRequestConfig,
                                       @Property(name = "gcp.project-id") String projectId) {
        this.client = client;
        this.transportChannelProvider = transportChannelProvider;
        this.credentialsProvider = credentialsProvider;
        this.pubSubResponseConfig = pubSubResponseConfig;
        this.pubSubRequestConfig = pubSubRequestConfig;
        this.projectId = projectId;
    }

    @PostConstruct
    public void init() {
        try (TopicAdminClient topicAdminClient = getTopicAdminClient()) {
            PubSubAdmin.createTopicIfNotExists(topicAdminClient, projectId, pubSubRequestConfig.getTopic());
            PubSubAdmin.createTopicIfNotExists(topicAdminClient, projectId, pubSubResponseConfig.getTopic());
        }
        try (SubscriptionAdminClient subscriptionAdminClient = getSubscriptionAdminClient()) {
            PubSubAdmin.createSubscriptionIfNotExists(subscriptionAdminClient, projectId, pubSubRequestConfig.getTopic(), pubSubRequestConfig.getSubscription(), 30);
        }
    }

    public TopicAdminClient getTopicAdminClient() {
        try {
            return TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(transportChannelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SubscriptionAdminClient getSubscriptionAdminClient() {
        try {
            return SubscriptionAdminClient.create(
                    SubscriptionAdminSettings.newBuilder()
                            .setTransportChannelProvider(transportChannelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
