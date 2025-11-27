package hello1.koddata.net;

import hello1.koddata.exception.KException;
import hello1.koddata.io.ChannelState;

public class UserUploadFileState extends ChannelState {

    private long sessionId;
    private String fileName;
    public UserUploadFileState(int chunkSize, long sessionId, String fileName) {
        super(chunkSize);
    }

    @Override
    public void perform() throws KException {

    }
}
