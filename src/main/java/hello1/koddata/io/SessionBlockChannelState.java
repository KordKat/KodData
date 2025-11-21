package hello1.koddata.io;

import hello1.koddata.engine.DataName;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.SessionData;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.utils.Serializable;

public class SessionBlockChannelState extends ChannelState {

    private SessionManager sessionManager;
    private long sessionId;
    private DataName name; 

    public SessionBlockChannelState(int chunkSize, SessionManager sessionManager, long sessionId, String name, String index) {
        super(chunkSize);
        this.sessionManager = sessionManager;
        this.sessionId = sessionId;
        this.name = new DataName(name, index);
    }

    @Override
    public void perform() throws KException {
        if(bytesReceived >= payloadLength){
            //todo performing put it into session
            int wireId = payloadBuffer.getInt();
            Session session = sessionManager.getSession(sessionId);
            SessionData sd = session.getSessionData();
            Class<? extends Serializable> wire = Serializable.searchWireClass(wireId);
            try {
                Serializable serializable = wire.newInstance();
                byte[] buf = new byte[payloadBuffer.remaining()];
                payloadBuffer.get(buf);
                serializable.deserialize(buf);
                sd.assignVariable(name, serializable);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new KException(ExceptionCode.KD00000, e.getMessage());
            }
        }
    }
}
