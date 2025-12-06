package hello1.koddata.net;

import hello1.koddata.Main;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.io.ChannelState;
import hello1.koddata.sessions.Session;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Future;

public class UserUploadFileState extends ChannelState {

    private long sessionId;
    private String fileName;
    private long writePosition = 0;

    public UserUploadFileState(int chunkSize, long sessionId, String fileName) {
        super(chunkSize);
        this.sessionId = sessionId;
        this.fileName = fileName;
    }

    @Override
    public void perform() throws KException {
       payloadBuffer.flip();
        Path rootfs = Main.bootstrap.getRootPath();
        Session userSession = Main.bootstrap.getSessionManager().getSession(sessionId);
        Path path = rootfs.resolve(userSession.getUser().getUserData().name()).resolve(fileName);

        try (AsynchronousFileChannel asyncFileChannel = AsynchronousFileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {

            Future<Integer> writeResult = asyncFileChannel.write(payloadBuffer, writePosition);

            int bytesWritten = writeResult.get();

            writePosition += bytesWritten;

            payloadBuffer.clear();

        } catch (Exception e) {
            throw new KException(ExceptionCode.KD00000 ,"Failed to write file asynchronously");
        }
    }
}
