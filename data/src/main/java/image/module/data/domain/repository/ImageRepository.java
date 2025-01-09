package image.module.data.domain.repository;

import image.module.data.domain.Image;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ImageRepository extends JpaRepository<Image, UUID> {
    void deleteByStoredFileName(String storedFileName);

    Image findByCdnUrl(String cdnUrl);
  Optional<Image> findByStoredFileName(String storedFileName);

    Optional<Image> findByOriginalFileUUIDAndSize(@Param("originalFileUUID") UUID originalFileUUID,
                                                  @Param("size") Integer size);
}
