package image.module.convert.dto;

import lombok.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Getter
@Setter
@NoArgsConstructor
public class OriginalImageResponse {
    private String storedFileName;
    private Integer requestSize;
    private SseEmitter emitter;
}
