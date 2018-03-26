package com.pubsub.logback.appender;

import java.io.IOException;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import net.logstash.logback.encoder.CompositeJsonEncoder;
import net.logstash.logback.encoder.LogstashEncoder;

public class PubSubAppender extends AppenderBase<ILoggingEvent> {

	private CompositeJsonEncoder<ILoggingEvent> encoder;
	private String projectID;
	private String topicName;
	private Publisher publisher;

	@Override
	public void start() {
		if (this.encoder == null || projectID == null || topicName == null) {
			addError("No encoder set for the appender named [" + name + "].");
			return;
		}

		try {
			TopicName topic = TopicName.of(projectID, topicName);
			publisher = Publisher.newBuilder(topic).build();
		} catch (IOException e) {
		}
		super.start();
	}

	public void append(ILoggingEvent event) {
		final String message=new String(encoder.encode(event));
		// convert message to bytes
		ByteString data = ByteString.copyFromUtf8(message);
		PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

		// schedule a message to be published, messages are automatically
		// batched
		ApiFuture<String> future = publisher.publish(pubsubMessage);

		// add an asynchronous callback to handle success / failure
		ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

			public void onSuccess(String messageId) {
				System.out.println(messageId);
			}

			public void onFailure(Throwable throwable) {
				if (throwable instanceof ApiException) {
					ApiException apiException = ((ApiException) throwable);
					// details on the API exception
					System.out.println(apiException.getStatusCode().getCode());
					System.out.println(apiException.isRetryable());
				}
				System.out.println("Error publishing message : " + message);
			}
		});
	}

	public CompositeJsonEncoder<ILoggingEvent> getEncoder() {
		return encoder;
	}

	public void setEncoder(CompositeJsonEncoder<ILoggingEvent> encoder) {
		this.encoder = encoder;
	}

	public String getProjectID() {
		return projectID;
	}

	public void setProjectID(String projectID) {
		this.projectID = projectID;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	
}