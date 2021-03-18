package tech.itpark;

import tech.itpark.entity.UserEntity;
import tech.itpark.repository.UserRepository;
import tech.itpark.repository.UserRepositoryJDBCImpl;
import tech.itpark.service.UserService;
import tech.itpark.service.UserServiceDefaultImpl;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class Main {
  public static void main(String[] args) {
    // жёсткая завязка на конкретный класс
    // я никак не могу повлиять на ЖЦ этого объекта
    // Singleton.getInstance();

    // JDBC connection URL
    // TODO: get from environment
    String dsn = "jdbc:postgresql://localhost:5400/appdb?user=app&password=pass";
    try (Connection connection = DriverManager.getConnection(dsn);) {
      UserRepository repository = new UserRepositoryJDBCImpl(connection);
      UserService service = new UserServiceDefaultImpl(repository);
      List<UserEntity> users = repository.findAll();

      boolean               removedByIdResult = repository.removeById(2L);
      Optional<UserEntity>  userById          = repository.findById(2L);
      Optional<UserEntity>  userByLogin       = repository.findByLogin("Admin");
      boolean               existUserByLogin  = repository.existsByLogin("User1+");

      String[] roles = {"ROLE_GUEST"};
      Timestamp timestamp = new Timestamp(System.currentTimeMillis());
      UserEntity userEntity = new UserEntity(0, "guest", "gst", "Guest", "guest", Set.of(roles), false, timestamp.toInstant().toEpochMilli());
      UserEntity newUserEntity = repository.save(userEntity);
      newUserEntity.setName("G.U.E.S.T");
      UserEntity updateUserEntity = repository.save(newUserEntity);

      System.out.println("Done!");

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}