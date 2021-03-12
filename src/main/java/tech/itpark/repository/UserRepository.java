package tech.itpark.repository;

import tech.itpark.entity.UserEntity;

import java.util.Optional;

public interface UserRepository extends CrudRepository<UserEntity, Long> {
  boolean existsByLogin(String login);
  Optional<UserEntity> findByLogin(String login);
}

//// CrudRepository
//public interface UserRepository {
//  // crud
//  List<UserEntity> findAll();
//  Optional<UserEntity> findById(long id); // Optional вместо UserEntity, потому что последний может быть null
//  // в старых системах всегда будет UserEntity = null (если не найдено)
//  UserEntity save(UserEntity entity);
//  boolean removeById(long id); // иногда void
//}
