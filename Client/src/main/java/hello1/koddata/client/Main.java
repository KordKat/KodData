package hello1.koddata.client;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        ServiceBootstrap bootstrap = new ServiceBootstrap();
        bootstrap.start(args);
    }
}