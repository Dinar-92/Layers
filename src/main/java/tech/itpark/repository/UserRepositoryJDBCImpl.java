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
                    Set.of((String[]) rs.getArray("roles").getArray()),
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
        final String queryString = "SELECT id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed FROM users ORDER BY id";
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
        if (id == null || id == 0) {
            return Optional.empty();
        }
        final String queryString = "SELECT id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed FROM users WHERE id = ?";
        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.ofNullable(mapper.map(rs));
                }
            }
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
        return Optional.of(null);
    }

    @Override
    public UserEntity save(UserEntity entity) {
        try (
                PreparedStatement stmt = connection.prepareStatement(
                        "INSERT INTO users (login, password, name, secret, roles, removed, created) VALUES (?, ?, ?, ?, ?, ?, ?)"
                )
        ) {
            Array array = connection.createArrayOf("TEXT", entity.getRoles().toArray());
            stmt.setString(1, entity.getLogin());
            stmt.setString(2, entity.getPassword());
            stmt.setString(3, entity.getName());
            stmt.setString(4, entity.getSecret());
            stmt.setArray(5, array);
            stmt.setBoolean(6, entity.isRemoved());
            stmt.setLong(7, entity.getCreated());

            stmt.execute();

        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
        return entity;
    }

    @Override
    public boolean removeById(Long id) {
        if (id == null || id == 0) {
            throw new IllegalArgumentException("Argument is empty");
        }
        final String queryString = "UPDATE users SET removed = TRUE WHERE id = ? " +
                "RETURNING id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed";
        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new DataAccessException("Empty result");
                }
                return mapper.map(rs).isRemoved();
            }
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    @Override
    public boolean existsByLogin(String login) {
        return findByLogin(login).isPresent();
    }

    @Override
    public Optional<UserEntity> findByLogin(String login) {
        final String loginParam = login.trim().toLowerCase();
        if (loginParam.isEmpty()) {
            return Optional.empty();
        }
        final String queryString = "SELECT id, login, password, name, secret, roles, EXTRACT(EPOCH FROM created) created, removed FROM users WHERE login = ?";
        try (
                PreparedStatement stmt = connection.prepareStatement(queryString);
        ) {
            stmt.setString(1, loginParam);
            try (ResultSet rs = stmt.executeQuery();) {
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