package tech.itpark.model;

// Data Class (Immutable) -> Record
public class ResetModel {
  private final String login;
  private final String newPassword;
  private final String secret; // имя любимого животного

  public ResetModel(String login, String newPassword, String secret) {
    this.login = login;
    this.newPassword = newPassword;
    this.secret = secret;
  }

  public String getLogin() {
    return login;
  }

  public String getNewPassword() {
    return newPassword;
  }

  public String getSecret() {
    return secret;
  }
}
