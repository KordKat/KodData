package hello1.koddata.io;

import hello1.koddata.Main;
import hello1.koddata.sessions.users.User;
import hello1.koddata.sessions.users.UserManager;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class UserFileChannelState extends ChannelState {

    private UserManager userManager;

    public UserFileChannelState(int chunkSize, UserManager userManager) {
        super(chunkSize);
        this.userManager = userManager;
    }

    @Override
    public void perform() {
        try {
            long userId = payloadBuffer.getLong();

            int nameLen = payloadBuffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            payloadBuffer.get(nameBytes);
            String fileName = new String(nameBytes, StandardCharsets.UTF_8);

            User user = userManager.findUser(userId);
            if(user != null){
                Path rootfs = Main.bootstrap.getRootPath();
                Path userPath = rootfs.resolve(user.getUserData().name());
                try(AsynchronousFileChannel afc = AsynchronousFileChannel.open(
                        userPath.resolve(fileName),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ)){
                    afc.write(payloadBuffer, 0);
                }
            }
        }catch (IOException ignored){
        }
    }
}