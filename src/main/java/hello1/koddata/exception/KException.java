package hello1.koddata.exception;

public class KException extends Exception {

    private final ExceptionCode errorCode;
    private final String msg;
    public KException(ExceptionCode errorCode, String msg){
        super(errorCode.getMessage());
        this.errorCode = errorCode;
        this.msg = msg;
    }

    public ExceptionCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return String.format("Error: %s -> %s, %s", errorCode.name(), errorCode.getMessage(), msg);
    }
}

