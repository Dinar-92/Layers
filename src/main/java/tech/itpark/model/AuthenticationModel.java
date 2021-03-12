package tech.itpark.model;

// Data Class (Immutable) -> Record
public class AuthenticationModel {
  private final String login;
  private final String password;

  public AuthenticationModel(String login, String password) {
    this.login = login;
    this.password = password;
  }

  public String getLogin() {
    return login;
  }

  public String getPassword() {
    return password;
  }
}
