package image.module.upload.presentation;

import image.module.upload.application.UploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/image/upload")
public class UploadController {

    private final UploadService uploadService;

    @PostMapping
    public SseEmitter uploadImage(@RequestParam("file") MultipartFile file,
                                  @RequestParam(value = "size") int size,
                                  @RequestParam(value = "cachingTime") int cachingTime
                                  ) {
        SseEmitter emitter = new SseEmitter(600000L);
        uploadService.handleUpload(file, size, cachingTime, emitter);
        return emitter;
    }

}
