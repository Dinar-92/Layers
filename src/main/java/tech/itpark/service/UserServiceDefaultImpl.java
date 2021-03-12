package tech.itpark.service;

import tech.itpark.entity.UserEntity;
import tech.itpark.exception.PasswordInvalidException;
import tech.itpark.exception.SecretInvalidException;
import tech.itpark.exception.UsernameAlreadyExistsException;
import tech.itpark.exception.UsernameNotExistsException;
import tech.itpark.model.*;
import tech.itpark.repository.UserRepository;

import java.time.OffsetDateTime;
import java.util.Set;

public class UserServiceDefaultImpl implements UserService {
  // Service - вышележащий слой
  // Поэтому он знает о нижележащем, но не наоборот
  private final UserRepository repository;

  public UserServiceDefaultImpl(UserRepository repository) {
    this.repository = repository;
  }

  @Override
  public UserModel register(RegistrationModel model) {
    if (repository.existsByLogin(model.getLogin())) {
      throw new UsernameAlreadyExistsException(model.getLogin());
    }

    // FIXME: remove manual mapping -> mapstruct
    UserEntity entity = repository.save(new UserEntity(
            0,
            model.getLogin(),
            // FIXME: hash password
            model.getPassword(),
            model.getName(),
            model.getSecret(),
            // FIXME: extract hardcoded roles
            Set.of("ROLE_USER"),
            false,
            OffsetDateTime.now().toEpochSecond() // long -> кол-во секунд с 1970 года 1 янв 00:00 по UTC
    ));

    return userModelByEntity(entity);
  }

  @Override
  public UserModel login(AuthenticationModel model) {
    UserEntity entity = repository
            .findByLogin(model.getLogin())
            .orElseThrow(() -> new UsernameNotExistsException(model.getLogin()));

    if (entity.isRemoved()) {
      throw new UsernameNotExistsException(model.getLogin());
    }

    if (!entity.getPassword().equals(model.getPassword())) {
      // username or password invalid
      // password invalid
      throw new PasswordInvalidException();
    }

    return userModelByEntity(entity);
  }

  @Override
  public UserModel reset(ResetModel model) {
    UserEntity entity = repository
            .findByLogin(model.getLogin())
            .orElseThrow(() -> new UsernameNotExistsException(model.getLogin()));

    if (!entity.getSecret().equals(model.getSecret())) {
      // Secret
      throw new SecretInvalidException();
    }

    entity.setPassword(model.getNewPassword());

    return userModelByEntity(entity);
  }

  @Override
  public boolean remove(RemovalModel model) {
    UserEntity entity = repository
            .findByLogin(model.getLogin())
            .orElseThrow(() -> new UsernameNotExistsException(model.getLogin()));

    if (!entity.getPassword().equals(model.getPassword())) {
      // username or password invalid
      // password invalid
      throw new PasswordInvalidException();
    }

    if (entity.isRemoved()) {
      return false;
    }

    entity.setRemoved(true);

    return true;
  }

  private UserModel userModelByEntity(UserEntity entity){
    UserModel result = null;
    if (entity!=null){
      result = new UserModel(
              entity.getId(),
              entity.getLogin(),
              entity.getName(),
              entity.getRoles(),
              entity.isRemoved(),
              entity.getCreated());
    }
    return result;
  }

}