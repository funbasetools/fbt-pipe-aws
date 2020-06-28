package com.funbasetools.pipes.aws;

import com.funbasetools.pipes.Message;

public class AwsMessage extends Message<String> {

    public static AwsMessage of(final String id, final String body) {
        return new AwsMessage(id, body);
    }

    protected AwsMessage(final String id, final String body) {
        super(id, body);
    }
}
