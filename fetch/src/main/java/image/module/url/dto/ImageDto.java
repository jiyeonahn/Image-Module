package image.module.url.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ImageDto {

    @JsonProperty("fileName")
    private String fileName;

    public ImageDto(String fileName) {
        this.fileName = fileName;
    }
}
