package tech.itpark.exception;

// FIXME: add parent exception to all application exceptions
public class UsernameAlreadyExistsException extends RuntimeException {
  public UsernameAlreadyExistsException() {
  }

  public UsernameAlreadyExistsException(String message) {
    super(message);
  }

  public UsernameAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public UsernameAlreadyExistsException(Throwable cause) {
    super(cause);
  }

  public UsernameAlreadyExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
