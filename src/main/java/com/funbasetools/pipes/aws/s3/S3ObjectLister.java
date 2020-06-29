package com.funbasetools.pipes.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.funbasetools.collections.Stream;
import com.funbasetools.collections.Streams;
import java.util.List;
import lombok.Builder;

@Builder
public class S3ObjectLister {

    private final AmazonS3Client s3Client;
    private final String bucketName;
    private final int maxListSize;

    public Stream<S3ObjectSummary> listObjects() {
        return fromPage("");
    }

    private Stream<S3ObjectSummary> fromPage(final String continuationToken) {

        final ListObjectsV2Request request = new ListObjectsV2Request()
            .withBucketName(bucketName)
            .withMaxKeys(maxListSize)
            .withContinuationToken(continuationToken);

        final ListObjectsV2Result res = s3Client.listObjectsV2(request);
        final List<S3ObjectSummary> list = res.getObjectSummaries();
        final String nextContinuationToken = res.getNextContinuationToken();

        if (list.isEmpty()) {
            return Streams.emptyStream();
        }

        return Streams
            .fromIterable(list)
            .append(() -> fromPage(nextContinuationToken));
    }
}
