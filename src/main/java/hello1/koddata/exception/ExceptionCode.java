package hello1.koddata.exception;

public enum ExceptionCode {

    KD00000("Regular Exception"),
    KDC0001("Unknown Token");

    private final String message;

    ExceptionCode(String message){
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

