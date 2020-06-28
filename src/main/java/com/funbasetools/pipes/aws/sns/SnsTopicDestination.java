package com.funbasetools.pipes.aws.sns;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishRequest;
import com.funbasetools.pipes.aws.AwsMessage;
import com.funbasetools.pipes.aws.AwsPipeDestination;
import lombok.Builder;

@Builder
public final class SnsTopicDestination implements AwsPipeDestination<AwsMessage> {

    private final AmazonSNS snsClient;
    private final String topicArn;

    @Override
    public void consume(final AwsMessage message) {
        final PublishRequest request = new PublishRequest()
            .withTopicArn(topicArn)
            .withMessage(message.getBody());

        snsClient.publish(request);
    }
}
