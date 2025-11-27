package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.engine.Bootstrap;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class UserManager {
    private Map<Long, User> users;
    private Map<Long, UserData> userDataMap;
    private Map<String, Long> userIdNameMap;
    private Bootstrap bootstrap;
    private File userFile;
    private Path userFilePath;

    private static Properties props;

    public UserManager(Bootstrap bootstrap){
        users = new ConcurrentHashMap<>();
        userDataMap = new ConcurrentHashMap<>();
        userIdNameMap = new ConcurrentHashMap<>();
        this.bootstrap = bootstrap;
        this.userFilePath = bootstrap.getRootPath().resolve("home");
    }

    public User logIn(long userId ,String password){
        if(userDataMap.containsKey(userId)){
            UserData userData = userDataMap.get(userId);
            if(userData.password().equals(password)){
                if(users.containsKey(userId)) return users.get(userId);
                User user;
                if(userData.isAdmin()){
                    user = new Admin(userData, this, bootstrap.getSessionManager());
                }else {
                    user = new User(userData);
                }

                users.put(userId, user);
                return user;
            }
        }
        return null;
    }

    public long getUserId(String username) {
        return userIdNameMap.getOrDefault(username, -1L);
    }

    public void createUser(UserData data) throws KException {
        if(userDataMap.containsKey(data.userId())){
            throw new KException(ExceptionCode.KD00000, "user id already taken");
        }

        this.userDataMap.put(data.userId(), data);

    }

    public User findUser(long userId){
        return users.get(userId);
    }

    public List<User> userList(){
        return users.values().stream().toList();
    }



    public void logoutUser(long userId){
        if(users.containsKey(userId)){
            User user = users.get(userId);
            for(Session session : user.listSessions()){
                session.terminate();
            }
        }
    }

    public void loadAllUserData() throws IOException, KException {
        if(!Files.exists(userFilePath)) Files.createDirectories(userFilePath);
        List<Path> userPaths = null;
        try(Stream<Path> stream = Files.list(userFilePath)){
            userPaths = stream.filter(Files::isDirectory).toList();
        } catch (RuntimeException e) {
            throw new KException(ExceptionCode.KD00010, e.getMessage());
        }
        for(Path path : userPaths){
            Path cfg = path.resolve("usr.cfg");
            if(!Files.exists(cfg)) continue;
            byte[] b = Files.readAllBytes(cfg);

            byte[][] fields = ByteUtils.splitBytes(b, (byte)0);
            assert fields.length == 8;
            byte[] userIdField = fields[0];
            assert userIdField.length == 8;
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.put(userIdField);
            buffer.flip();
            long userId = buffer.getLong();
            String name = new String(fields[1], StandardCharsets.UTF_8);
            String password = new String(fields[2], StandardCharsets.UTF_8);
            assert fields[3].length == 1;
            boolean isAdmin = fields[3][0] != 0;
            // userPrivileges
            int maxSession = ByteUtils.bytesToInt(fields[4]);
            int maxProcessPerSession = ByteUtils.bytesToInt(fields[5]);
            int maxMemoryPerProcess = ByteUtils.bytesToInt(fields[6]);
            int maxStorageUsage = ByteUtils.bytesToInt(fields[7]);
            UserPrivilege privilege = new UserPrivilege(maxSession, maxProcessPerSession, maxMemoryPerProcess, maxStorageUsage);
            UserData userData = new UserData(userId, name, privilege, password, isAdmin);
            this.userDataMap.put(userId, userData);
        }
    }

    public void saveUserData() throws IOException {
        if (!Files.exists(userFilePath)) {
            Files.createDirectories(userFilePath);
        }

        for (UserData userData : userDataMap.values()) {
            Path userDir = userFilePath.resolve(userData.name());
            if (!Files.exists(userDir)) {
                Files.createDirectories(userDir);
            }
            Path cfg = userDir.resolve("usr.cfg");

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(userData.userId());
            byte[] userIdBytes = buffer.array();

            byte[] nameBytes = userData.name().getBytes(StandardCharsets.UTF_8);
            byte[] passwordBytes = userData.password().getBytes(StandardCharsets.UTF_8);
            byte[] isAdminBytes = new byte[] { (byte) (userData.isAdmin() ? 1 : 0) };

            UserPrivilege priv = userData.userPrivilege();
            byte[] maxSessionBytes = ByteUtils.intToBytes(priv.maxSession());
            byte[] maxProcessBytes = ByteUtils.intToBytes(priv.maxProcessPerSession());
            byte[] maxMemoryBytes = ByteUtils.intToBytes(priv.maxMemoryPerProcess());
            byte[] maxStorageBytes = ByteUtils.intToBytes(priv.maxStorageUsage());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            baos.write(userIdBytes);
            baos.write(0);

            baos.write(nameBytes);
            baos.write(0);

            baos.write(passwordBytes);
            baos.write(0);

            baos.write(isAdminBytes);
            baos.write(0);

            baos.write(maxSessionBytes);
            baos.write(0);

            baos.write(maxProcessBytes);
            baos.write(0);

            baos.write(maxMemoryBytes);
            baos.write(0);

            baos.write(maxStorageBytes);

            byte[] finalBytes = baos.toByteArray();

            Files.write(cfg, finalBytes);
        }
    }

    public void changeUserPassword(long userId, String newPass){
        if(userDataMap.containsKey(userId)){
            userDataMap.computeIfPresent(userId, (k, userData) -> userData.withPassword(newPass));
        }
    }

    public void removeUser(long id){
        if(users.containsKey(id)) {
            User u = users.get(id);
            u.logOut();
            for(Session session : u.listSessions()){
                session.terminate();
            }
            userDataMap.remove(id);
            users.remove(id);
        }
    }

    public String getProperty(String key, String defaultValue){
        return props.getProperty(key, defaultValue);
    }

    public void setProperty(String key, String value){
        props.setProperty(key, value);
    }

    public void removeProperty(String key){
        props.remove(key);
    }

    public UserData getUserDataByName(String name){
        Optional<UserData> op =userDataMap.entrySet().stream().filter(x -> x.getValue().name().equals(name)).map(x -> x.getValue()).findAny();
        return op.orElse(null);
    }

}
