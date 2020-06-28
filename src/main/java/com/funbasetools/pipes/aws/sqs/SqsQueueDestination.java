package com.funbasetools.pipes.aws.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.funbasetools.pipes.aws.AwsPipeDestination;
import lombok.Builder;

@Builder
public class SqsQueueDestination implements AwsPipeDestination<SqsMessage> {

    private final AmazonSQS sqsClient;
    private final String queueUrl;

    @Override
    public void consume(final SqsMessage message) {
        final SendMessageRequest request = new SendMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageBody(message.getBody())
            .withMessageDeduplicationId(message.getId());

        sqsClient.sendMessage(request);
    }
}
