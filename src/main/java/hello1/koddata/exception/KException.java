package hello1.koddata.exception;

public class KException extends Exception {

    private final ExceptionCode errorCode;

    public KException(ExceptionCode errorCode){
        super(errorCode.getMessage());
        this.errorCode = errorCode;
    }

    public ExceptionCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return String.format("Error: %s -> %s", errorCode.name(), errorCode.getMessage());
    }
}

