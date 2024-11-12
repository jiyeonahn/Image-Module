
package image.module.resize.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ReceiveKafkaMessage {
  private String storedOriginalFileName;
  private String WebPFileName;
  private Integer Size;
}
