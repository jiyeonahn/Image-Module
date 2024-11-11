package image.module.upload.infrastructure;

import image.module.upload.application.ImageRequest;
import image.module.upload.application.ImageResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "data")
public interface DataClient extends DataService {

    @PostMapping("/image/upload")
    ImageResponse saveImageOriginalData(@RequestBody ImageRequest imageRequest);
}
