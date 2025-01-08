package image.module.url;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class FetchApplication {

	public static void main(String[] args) {
		SpringApplication.run(FetchApplication.class, args);
	}

}
