package tech.itpark.model;

import java.util.Set;

public class UserModel {
  private final long id;
  private final String login;
  private final String name;
  private final Set<String> roles;
  private final boolean removed;
  private final long created;

  public UserModel(long id, String login, String name, Set<String> roles, boolean removed, long created) {
    this.id = id;
    this.login = login;
    this.name = name;
    this.roles = roles;
    this.removed = removed;
    this.created = created;
  }

  public long getId() {
    return id;
  }

  public String getLogin() {
    return login;
  }

  public String getName() {
    return name;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public boolean isRemoved() {
    return removed;
  }

  public long getCreated() {
    return created;
  }
}
