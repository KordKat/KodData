package hello1.koddata.exception;

public enum ExceptionCode {

    KD00000("Regular Exception"),
    KDC0001("Unknown Token"),
    KDC0002("Unexpected Token"),
    KDC0003("Parsing error"),
    KDM0004("Memory allocation error"),
    KD00005("IllegalArgumentException"),
    KD00006("IllegalStateException");

    private final String message;

    ExceptionCode(String message){
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

