package hello1.koddata.sessions;

public class SessionSettings {

    private int maxVisitorInSession = -1; // this session can have more than 1 visitor at a time
    private int maxSessionProcess = -1; // this session can have unlimited process
    private boolean interruptableSession = false; // this session cannot be interrupted by higher priority session except admin
    private boolean canInterruptAnotherSession = false; // this session cannot interrupt lower priority session
    private long maximumAllocatedMemoryByte = -1;
}
