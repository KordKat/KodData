package hello1.koddata.io;

import hello1.koddata.engine.DataName;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.SessionData;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.utils.Serializable;

import java.nio.charset.StandardCharsets;

public class SessionBlockChannelState extends ChannelState {

    private SessionManager sessionManager;

    public SessionBlockChannelState(int chunkSize, SessionManager sessionManager) {
        super(chunkSize);
        this.sessionManager = sessionManager;
    }

    @Override
    public void perform() throws KException {
        try {
            long sessionId = payloadBuffer.getLong();

            int nameLen = payloadBuffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            payloadBuffer.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);

            int indexLen = payloadBuffer.getInt();
            byte[] indexBytes = new byte[indexLen];
            payloadBuffer.get(indexBytes);
            String index = new String(indexBytes, StandardCharsets.UTF_8);

            int wireId = payloadBuffer.getInt();

            Session session = sessionManager.getSession(sessionId);
            if(session != null){
                SessionData sd = session.getSessionData();
                Class<? extends Serializable> wire = Serializable.searchWireClass(wireId);
                Serializable serializable = wire.newInstance();
                byte[] buf = new byte[payloadBuffer.remaining()];
                payloadBuffer.get(buf);
                serializable.deserialize(buf);
                sd.assignVariable(new DataName(name, index), serializable);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new KException(ExceptionCode.KD00000, e.getMessage());
        }
    }
}