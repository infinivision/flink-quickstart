package org.infinivision.flink.io;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

public class CephS3Demo {
    public static void main(String[] args) {
        String accessKey = "HYR0DJYTENCIL2HYAMZ1";
        String secretKey = "KQpl95qvokPktKZ1xpRovTRVQGe6k7szG0UfVa6r";
        String endPoint = "172.19.0.103:7480";
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);

        conn.setEndpoint(endPoint);

        // list buckets
        System.out.println("==== list buckets");
        List<Bucket> buckets = conn.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName() + "\t" +
                    StringUtils.fromDate(bucket.getCreationDate()));
        }

        System.out.println("==== list objects in test-bucket");
        ObjectListing objects = conn.listObjects("test-bucket");

        for(S3ObjectSummary summary : objects.getObjectSummaries()) {
            System.out.println(summary.getKey() + "\t" + summary.getSize());
        }


        // put a object
        System.out.println("==== put object to test-bucket");
        ByteArrayInputStream input = new ByteArrayInputStream("Hello World!".getBytes());
        conn.putObject("test-bucket", "hello.txt", input, new ObjectMetadata());

        // list object again
        System.out.println("==== list objects in test-bucket");
        objects = conn.listObjects("test-bucket");

        for(S3ObjectSummary summary : objects.getObjectSummaries()) {
            System.out.println(summary.getKey() + "\t" + summary.getSize());
        }
    }
}
