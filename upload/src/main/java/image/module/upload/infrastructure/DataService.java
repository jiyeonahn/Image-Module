package image.module.upload.infrastructure;

import image.module.upload.application.ImageRequest;
import image.module.upload.application.ImageResponse;
import org.springframework.web.bind.annotation.RequestBody;

public interface DataService {

    ImageResponse saveImageOriginalData(ImageRequest imageRequest);

    void deleteImageData(String storedFileName);
}
