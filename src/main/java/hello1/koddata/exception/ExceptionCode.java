package hello1.koddata.exception;

public enum ExceptionCode {

    KD00000("Regular Exception"),
    KDC0001("Unknown Token"),
    KDC0002("Unexpected Token"),
    KDC0003("Parsing error");

    private final String message;

    ExceptionCode(String message){
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

