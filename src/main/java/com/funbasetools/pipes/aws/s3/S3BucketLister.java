package com.funbasetools.pipes.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.funbasetools.collections.Stream;
import com.funbasetools.collections.Streams;
import lombok.Builder;

@Builder
public class S3BucketLister {

    private final AmazonS3 s3Client;

    public Stream<Bucket> listBuckets() {
        return Streams.of(s3Client.listBuckets());
    }
}
