package tech.itpark.jdbc;

import java.sql.ResultSet;

public interface RowMapper<T> {
    T map(ResultSet rs); // no checked exception
}