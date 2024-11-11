package image.module.upload.application;

import image.module.upload.util.FileUtil;
import image.module.upload.domain.ImageExtension;
import image.module.upload.infrastructure.DataService;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import javax.imageio.ImageIO;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@Service
@RequiredArgsConstructor
public class UploadService {
    private final MinioClient minioClient;
    private final DataService dataService;
    private final KafkaTemplate<String, ImageUploadMessage> kafkaTemplate;

    @Value("${minio.bucket}")
    private String bucketName;

    public void handleUpload(MultipartFile file, int size, int cachingTime, SseEmitter emitter) {
        try {
            emitter.send(SseEmitter.event().name("INIT").data("이미지 업로드가 시작되었습니다."));
            uploadOriginalImage(file, size, cachingTime).handle((result, ex) -> {
                try {
                    if (ex == null) {
                        emitter.send(SseEmitter.event().name("SUCCESS").data("이미지 업로드 완료: " + result));
                        emitter.complete();
                    } else {
                        emitter.send(SseEmitter.event().name("ERROR").data(file.getOriginalFilename() + " 업로드 실패..." + ex.getMessage()));
                        emitter.completeWithError(ex);
                    }
                } catch (IOException e) {
                    log.error("IOException 발생", e);
                    emitter.completeWithError(e);
                }
                return null;
            });
        } catch (IOException e) {
            log.error("IOException 발생", e);
            try {
                emitter.send(SseEmitter.event().name("ERROR").data("업로드 실패..."));
            } catch (IOException ioException) {
                log.error("SseEmitter 전송 오류", ioException);
            }
            emitter.completeWithError(e);
        }
    }

    //이미지 데이터 db 저장
    public CompletableFuture<String> uploadOriginalImage(MultipartFile file, int requestSize, int cachingTime) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 업로드 파일명을 불러옴
                String originalName = file.getOriginalFilename();

                // 파일 이름이 존재하지 않는 경우
                if (originalName == null) throw new IllegalArgumentException("잘못된 파일입니다.");

                // 파일 정보 분리
                String[] fileInfos = FileUtil.splitFileName(originalName);

                // 확장자 체크
                ImageExtension extension = ImageExtension.findByKey(fileInfos[1])
                        .orElseThrow(() -> new Exception("지원하지 않는 확장자 입니다."));

                // createImageRequest
                ImageRequest imageRequest = createImageRequest(file, cachingTime, originalName, extension);

                String storedFileName = imageRequest.getStoredFileName();

                uploadImageToMinio(file.getInputStream(), file.getSize(), file.getContentType(), storedFileName);

                // 메타데이터 저장
                ImageResponse imageResponse = dataService.saveImageOriginalData(imageRequest);

                kafkaTemplate.send("image-upload-topic", ImageUploadMessage.createMessage(storedFileName,requestSize));

                return imageResponse.getOriginalFileUUID().toString();
            } catch (Exception e) {
                log.error("이미지 메타데이터 저장 중 오류 발생: ", e);
                throw new RuntimeException(e);
            }
        });
    }

    private static ImageRequest createImageRequest(MultipartFile file, int cachingTime, String originalName,
                                                ImageExtension extension) throws IOException {
        BufferedImage bufferedImage = ImageIO.read(file.getInputStream());
        int imageWidth = bufferedImage.getWidth();
        int imageHeight = bufferedImage.getHeight();
        int imageSize = Math.max(imageWidth, imageHeight);

        return ImageRequest.create(
                originalName,
                extension.getKey(),
                imageSize,
                cachingTime
        );
    }

    //이미지 업로드
    @SneakyThrows
    public void uploadImageToMinio(InputStream fileInputStream, long size, String contentType, String storedFileName) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(storedFileName)
                            .stream(fileInputStream, size, -1)
                            .contentType(contentType)
                            .build()
            );
    }
}
