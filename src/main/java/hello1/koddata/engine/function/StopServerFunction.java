package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.users.Admin;
import hello1.koddata.sessions.users.User;

import java.io.IOException;

//Inheritance
public class StopServerFunction extends KodFunction<Integer>{

//    Polymorphism
    @Override
    public Value<Integer> execute() throws KException {
        Value<?> userVal = arguments.get("userId");
        if(!(userVal.get() instanceof Number id)){
            throw new KException(ExceptionCode.KDE0012, "id should be number");
        }
        User user = Main.bootstrap.getUserManager().findUser(id.longValue());
        if(!(user instanceof Admin admin)){
            throw new KException(ExceptionCode.KD00000, "No Permission");
        }

        try {
            Main.bootstrap.end();
        } catch (IOException | InterruptedException e) {
            throw new KException(ExceptionCode.KD00000, e.getMessage());
        }
        return new Value<>(0);
    }
}
