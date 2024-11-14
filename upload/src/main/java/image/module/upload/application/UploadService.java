package image.module.upload.application;

import image.module.upload.util.FileUtil;
import image.module.upload.domain.ImageExtension;
import image.module.upload.infrastructure.DataService;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.imageio.ImageIO;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@Service
@RequiredArgsConstructor
public class UploadService {
    private final MinioClient minioClient;
    private final DataService dataService;
    private final KafkaTemplate<String, ImageUploadMessage> kafkaTemplate;
    private SseEmitter sseEmitter;

    @Value("${minio.bucket}")
    private String bucketName;

    public void handleUpload(MultipartFile file, int size, int cachingTime, SseEmitter emitter) {
        try {
            sseEmitter = emitter;
            sseEmitter.send(SseEmitter.event().name("INIT").data("이미지 업로드가 시작되었습니다."));
            uploadOriginalImage(file, cachingTime).handle((result, ex) -> {
                try {
                    if (ex == null) {
                        sseEmitter.send(SseEmitter.event().name("1.ORIGINAL").data("이미지 원본 업로드 처리중입니다.: " + result.getOriginalFileUUID()));
                        kafkaTemplate.send("image-convert-topic", ImageUploadMessage.createMessage(
                                result.getStoredFileName(), size));
                    } else {
                        sseEmitter.send(SseEmitter.event().name("ERROR").data(file.getOriginalFilename() + " 업로드 실패..." + ex.getMessage()));
                        sseEmitter.completeWithError(ex);
                    }
                } catch (IOException e) {
                    log.error("IOException 발생", e);
                    sseEmitter.completeWithError(e);
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
    public CompletableFuture<ImageResponse> uploadOriginalImage(MultipartFile file, int cachingTime)
            throws IOException {
        AtomicReference<String> storedFileName = new AtomicReference<>();
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

                storedFileName.set(imageRequest.getStoredFileName());

                uploadImageToMinio(file.getInputStream(), file.getSize(), file.getContentType(), storedFileName.get());

                return dataService.saveImageOriginalData(imageRequest);
            } catch (Exception e) {
                log.error("IMAGE UPLOAD FAIL!! ", e);
                try {
                    this.rollbackUpload(storedFileName.get());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
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

    @KafkaListener(topics = "convert-complete", groupId = "image-upload-group")
    public void onConvertComplete(String message) throws IOException {
        sseEmitter.send(SseEmitter.event().name("2.CONVERT").data(message));
    }

    @KafkaListener(topics = "resize-complete", groupId = "image-upload-group")
    public void onResizeComplete(String message) throws IOException {
        sseEmitter.send(SseEmitter.event().name("3.RESIZE").data(message));
        sseEmitter.send(SseEmitter.event().name("SUCCESS").data("이미지 업로드가 완료되었습니다."));
        sseEmitter.complete();
    }

    @KafkaListener(topics = "image-upload-rollback", groupId = "image-upload-group")
    public void rollbackUpload(String storedFileName) throws Exception {
        String sanitizedFileName = storedFileName.replace("\"", "");
        log.error("UPLOAD ROLLBACK! {}", sanitizedFileName);

        //데이터 삭제
        dataService.deleteImageData(sanitizedFileName);
        log.info("DELETE IMAGE DATA!!");

        //스토리지에서 파일 삭제
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucketName)
                        .object(sanitizedFileName)
                        .build()
        );
        log.info("DELETE MINIO DATA!!");

        sseEmitter.send(SseEmitter.event().name("ERROR").data(" 이미지 업로드를 실패했습니다."));
        sseEmitter.complete();
    }
}
