package image.module.upload.infrastructure;

import image.module.upload.application.ImageRequest;
import image.module.upload.application.ImageResponse;

public interface DataService {
    ImageResponse saveImageOriginalData(ImageRequest imageRequest);
}
