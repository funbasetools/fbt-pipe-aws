package com.funbasetools.pipes.aws.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.funbasetools.Try;
import com.funbasetools.pipes.aws.AwsPipeSource;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;

@Builder
public class SqsQueueSource implements AwsPipeSource<SqsMessage> {

    private final AmazonSQS sqsClient;
    private final String queueUrl;
    private final int batchSize;
    private final int readWaitTimeInSeconds;

    @Override
    public List<SqsMessage> get(int count) {
        return Try
            .of(() -> {
                final ReceiveMessageRequest request = new ReceiveMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMaxNumberOfMessages(batchSize)
                    .withAttributeNames("All")
                    .withWaitTimeSeconds(readWaitTimeInSeconds);

                return sqsClient
                    .receiveMessage(request)
                    .getMessages()
                    .stream()
                    .map(SqsMessage::from)
                    .collect(Collectors.toList());
            })
            .toOptional()
            .orElse(Collections.emptyList());
    }

    @Override
    public void acknowledge(final SqsMessage message) {
        final DeleteMessageRequest request = new DeleteMessageRequest()
            .withQueueUrl(queueUrl)
            .withReceiptHandle(message.getReceiptHandle());

        sqsClient.deleteMessage(request);
    }
}
