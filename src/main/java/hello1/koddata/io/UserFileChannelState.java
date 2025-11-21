package hello1.koddata.io;

import hello1.koddata.sessions.users.User;
import hello1.koddata.sessions.users.UserManager;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class UserFileChannelState extends ChannelState {

    private String fileName;
    private long userId;
    private Path rootfs;
    private UserManager userManager;

    public UserFileChannelState(int chunkSize, String fileName, long userId, Path rootfs, UserManager userManager) {
        super(chunkSize);
        this.fileName = fileName;
        this.userId = userId;
        this.rootfs = rootfs;
        this.userManager = userManager;
    }

    @Override
    public void perform() {
        if(bytesReceived >= payloadLength){
            User user = userManager.findUser(userId);
            Path userPath = rootfs.resolve(user.getUserData().name());

            try(AsynchronousFileChannel afc = AsynchronousFileChannel.open(
                    userPath.resolve(fileName),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ)){
                payloadBuffer.flip();
                afc.write(payloadBuffer, 0);
            }catch (IOException ex){

            }


        }
    }
}
