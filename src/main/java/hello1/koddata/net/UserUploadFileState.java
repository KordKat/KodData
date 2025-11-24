package hello1.koddata.net;

import hello1.koddata.exception.KException;
import hello1.koddata.io.ChannelState;

public class UserUploadFileState extends ChannelState {

    private long sessionId;

    public UserUploadFileState(int chunkSize, long sessionId) {
        super(chunkSize);
    }

    @Override
    public void perform() throws KException {

    }
}
