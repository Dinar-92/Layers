package tech.itpark.repository;

import java.util.List;
import java.util.Optional;

// CrudRepository<T, ID>
public interface CrudRepository<T, ID> {
  // crud
  List<T> findAll();
  Optional<T> findById(ID id); // Optional вместо T, потому что последний может быть null
  // в старых системах всегда будет T = null (если не найдено)
  T save(T entity);
  boolean removeById(ID id); // иногда void
}
