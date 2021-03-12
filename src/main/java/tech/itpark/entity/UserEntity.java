package tech.itpark.entity;

import java.util.Set;

public class UserEntity {
  private long id;
  private String login;
  private String password;
  private String name;
  private String secret;
  private Set<String> roles;
  private boolean removed;
  private long created;

  public UserEntity() {
  }

  public UserEntity(long id, String login, String password, String name, String secret, Set<String> roles, boolean removed, long created) {
    this.id = id;
    this.login = login;
    this.password = password;
    this.name = name;
    this.secret = secret;
    this.roles = roles;
    this.removed = removed;
    this.created = created;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getLogin() {
    return login;
  }

  public void setLogin(String login) {
    this.login = login;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSecret() {
    return secret;
  }

  public void setSecret(String secret) {
    this.secret = secret;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public void setRoles(Set<String> roles) {
    this.roles = roles;
  }

  public boolean isRemoved() {
    return removed;
  }

  public void setRemoved(boolean removed) {
    this.removed = removed;
  }

  public long getCreated() {
    return created;
  }

  public void setCreated(long created) {
    this.created = created;
  }

}
