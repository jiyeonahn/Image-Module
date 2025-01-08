package image.module.url.config;

import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MinioConfig {

    @Value("${minio.url}")
    private String minioUrl;

    @Value("${minio.access.key}")  // 수정된 부분
    private String accessKey;

    @Value("${minio.access.secret}")  // 수정된 부분
    private String secretKey;

    @Value("${minio.buckets.downloadBucket}") // 다운로드 버킷 이름
    private String downloadBucket;

    @Value("${minio.buckets.uploadBucket}")   // 업로드 버킷 이름
    private String uploadBucket;

    @Bean
    public MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint("http://minio:9000")
                .credentials(accessKey, secretKey)
                .build();
    }

    public String getDownloadBucket() {
        return downloadBucket;
    }

    public String getUploadBucket() {
        return uploadBucket;
    }
}
