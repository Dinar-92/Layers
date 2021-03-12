package tech.itpark.exception;

public class SecretInvalidException extends RuntimeException {
    public SecretInvalidException() {
    }

    public SecretInvalidException(String message) {
        super(message);
    }

    public SecretInvalidException(String message, Throwable cause) {
        super(message, cause);
    }

    public SecretInvalidException(Throwable cause) {
        super(cause);
    }

    public SecretInvalidException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
