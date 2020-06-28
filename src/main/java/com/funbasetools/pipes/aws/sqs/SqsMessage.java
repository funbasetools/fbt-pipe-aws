package com.funbasetools.pipes.aws.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.funbasetools.pipes.aws.AwsMessage;

public class SqsMessage extends AwsMessage {

    private final String receiptHandle;

    public static SqsMessage from(final Message message) {
        return new SqsMessage(message.getMessageId(), message.getReceiptHandle(), message.getBody());
    }

    public static SqsMessage of(final String id, final String receiptHandle, final String body) {
        return new SqsMessage(id, receiptHandle, body);
    }

    private SqsMessage(final String id, final String receiptHandle, final String body) {
        super(id, body);
        this.receiptHandle = receiptHandle;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }
}
