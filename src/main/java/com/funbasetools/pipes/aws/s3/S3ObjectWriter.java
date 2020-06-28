package com.funbasetools.pipes.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.funbasetools.Try;
import com.funbasetools.Unit;
import com.funbasetools.io.Writer;
import lombok.Builder;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class S3ObjectWriter implements Writer {

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final int bufferSize;

    @Builder
    private S3ObjectWriter(final AmazonS3 s3Client, final String bucketName) {
        this(s3Client, bucketName, 2 * 1024 * 1024);
    }

    @Builder
    private S3ObjectWriter(
        final AmazonS3 s3Client,
        final String bucketName,
        final int bufferSize) {

        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.bufferSize = bufferSize;
    }

    @Override
    public BufferedOutputStream openWriteStream(final String objectPath) throws IOException {
        try {
            final InitiateMultipartUploadResult initiateMultipartUploadResult =
                s3Client.initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(
                        bucketName,
                        objectPath,
                        new ObjectMetadata()
                    )
                );

            return new BufferedOutputStream(
                new OutputStream() {
                    private final List<PartETag> partETagList = new LinkedList<>();

                    @Override
                    public void write(final int integer) throws IOException {
                        final byte[] bytes = ByteBuffer
                            .allocate(Integer.BYTES)
                            .putInt(integer)
                            .array();

                        write(bytes, 0, bytes.length);
                    }

                    @Override
                    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
                        try {
                            final UploadPartRequest uploadPartRequest = new UploadPartRequest()
                                .withBucketName(bucketName)
                                .withKey(objectPath)
                                .withUploadId(initiateMultipartUploadResult.getUploadId())
                                .withInputStream(new ByteArrayInputStream(bytes, offset, length))
                                .withPartSize(length);

                            final UploadPartResult uploadPartResult = s3Client.uploadPart(uploadPartRequest);
                            partETagList.add(uploadPartResult.getPartETag());
                        }
                        catch (Exception ex) {
                            throw new IOException(ex);
                        }
                    }

                    @Override
                    public void flush() {
                    }

                    @Override
                    public void close() throws IOException {
                        try {
                            flush();
                            final CompleteMultipartUploadRequest completeMultipartUploadRequest =
                                new CompleteMultipartUploadRequest(
                                    bucketName,
                                    objectPath,
                                    initiateMultipartUploadResult.getUploadId(),
                                    partETagList
                                );

                            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
                        }
                        catch (Exception ex) {
                            throw new IOException(ex);
                        }
                    }
                },
                bufferSize
            );
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Try<Unit> writeAllBytes(String objectPath, byte[] bytes) {
        return Try.of(() -> {
            final PutObjectRequest request = new PutObjectRequest(
                bucketName,
                objectPath,
                new ByteArrayInputStream(bytes),
                new ObjectMetadata()
            );

            s3Client.putObject(request);
        });
    }
}
