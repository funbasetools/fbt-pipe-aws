package com.funbasetools.pipes.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.funbasetools.io.Reader;
import lombok.Builder;

@Builder
public class S3ObjectReader implements Reader {

    private final AmazonS3 s3Client;
    private final String bucketName;

    @Override
    public S3ObjectInputStream openReadStream(final String objectPath) {
        return s3Client
            .getObject(bucketName, objectPath)
            .getObjectContent();
    }
}
