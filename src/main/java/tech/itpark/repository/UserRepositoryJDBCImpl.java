package tech.itpark.repository;

import tech.itpark.entity.UserEntity;
import tech.itpark.exception.DataAccessException;
import tech.itpark.jdbc.RowMapper;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;


// Driver - iface
// Connection - iface
// Statement/PreparedStatement/CallableStatement - iface
// ResultSet - iface
// SQLException -> Exception (checked) - try-catch or signature
// типы SQL'ые

//class UserEntityRowMapper implements RowMapper<UserEntity> {
//
//  @Override
//  public UserEntity map(ResultSet rs) throws SQLException {
//    return new UserEntity(rs.getLong("id"), ...);
//  }
//}

// nested
// inner
// local
// anonymous

// alt + insert - generation
// alt + enter - make
public class UserRepositoryJDBCImpl implements UserRepository {
    private final Connection connection;
    private final RowMapper<UserEntity> mapper = rs -> {
        try {
            return new UserEntity(
                    rs.getLong("id"),
                    rs.getString("login"),
                    rs.getString("password"),
                    rs.getString("name"),
                    rs.getString("secret"),
                    Set.of((String[])rs.getArray("roles").getArray()),
                    rs.getBoolean("removed"),
                    rs.getLong("created")
            );
        } catch (SQLException e) {
            // pattern -> "convert" checked to unchecked (заворачивание исключений)
            throw new DataAccessException(e);
        }
    };

    public UserRepositoryJDBCImpl(Connection connection) {
        this.connection = connection;
    }

    // mapper -> map -> objectType1 -> objectType2:
    // rs -> UserEntity
    @Override
    public List<UserEntity> findAll() {
        String queryString = "SELECT id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed FROM users ORDER BY id";
        try (
                final Statement stmt = connection.createStatement();
                final ResultSet rs = stmt.executeQuery(queryString);
        ) {
            List<UserEntity> result = new LinkedList<>();
            while (rs.next()) {
                final UserEntity entity = mapper.map(rs);
                result.add(entity);
            }
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public Optional<UserEntity> findById(Long id) {
        if (id == null || id == 0){
            return Optional.empty();
        }
        String queryString = "SELECT id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed FROM users WHERE id = ?";
        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            stmt.setLong(1, id);
            try(ResultSet rs = stmt.executeQuery();){
                if (rs.next()) {
                    return Optional.ofNullable(mapper.map(rs));
                }
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public UserEntity save(UserEntity entity) {
        if (entity == null){
            throw new IllegalArgumentException("Argument is empty");
        }
        boolean insertUpdate = findById(entity.getId()).isEmpty();

        String queryString =  (insertUpdate ?
                "INSERT INTO users(login, password, name, secret, roles, created, removed) VALUES (?, ?, ?, ?, ?, ?, ?) ":
                "UPDATE users SET login = ?, password = ?, name = ?, secret = ?, roles = ?, created = ?, removed = ? WHERE id = ? ") +
                "RETURNING id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed";

        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            int index = 0;
            stmt.setString(++index, entity.getLogin());
            stmt.setString(++index, entity.getPassword());
            stmt.setString(++index, entity.getName());
            stmt.setString(++index, entity.getSecret());
            stmt.setArray(++index,  connection.createArrayOf("TEXT", entity.getRoles().toArray()));
            stmt.setTimestamp(++index, new Timestamp(entity.getCreated()) );
            stmt.setBoolean(++index, entity.isRemoved());
            if (!insertUpdate) {
                stmt.setLong(++index, entity.getId());
            }

            try(ResultSet rs = stmt.executeQuery()){
                if(!rs.next()){
                    throw new DataAccessException("Empty result");
                }
                return mapper.map(rs);
            }

        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public boolean removeById(Long id) {
        if(id == null || id == 0){
            throw new IllegalArgumentException("Argument is empty");
        }
        String queryString = "UPDATE users SET removed = TRUE WHERE id = ? "+
                "RETURNING id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed";
        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            stmt.setLong(1, id);
            try(ResultSet rs = stmt.executeQuery()){
                if(!rs.next()){
                    throw new DataAccessException("Empty result");
                }
                return mapper.map(rs).isRemoved();
            }
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public boolean existsByLogin(String login){
        return findByLogin(login).isPresent();
    }

    @Override
    public Optional<UserEntity> findByLogin(String login){
        String loginParam = login.trim().toLowerCase();
        if (loginParam.isEmpty()){
            return Optional.empty();
        }
        String queryString = "SELECT id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed FROM users WHERE login = ?";
        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            stmt.setString(1, loginParam);
            try(ResultSet rs = stmt.executeQuery();){
                if (rs.next()) {
                    return Optional.ofNullable(mapper.map(rs));
                }
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }
}