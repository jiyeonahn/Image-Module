package image.module.convert.dto;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SendKafkaMessage {
    private String storedOriginalFileName;
    private String WebPFileName;
    private Integer Size;

    public static SendKafkaMessage createMessage(String storedOriginalFileName, String fileName, int size){
        return SendKafkaMessage.builder()
                .storedOriginalFileName(storedOriginalFileName)
                .WebPFileName(fileName)
                .Size(size)
                .build();
    }
}
