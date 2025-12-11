package hello1.koddata.engine.function;

import hello1.koddata.engine.DataName;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.SessionData;

//Inheritance
public class RemoveFunction extends KodFunction<SessionData>{

//     Polymorphism
    @Override
    public Value<SessionData> execute() throws KException {
        if (!arguments.containsKey("dataName")){
            throw new KException(ExceptionCode.KDE0012,"Remove function need argument databaseName");
        }
        Value<?> dataName = arguments.get("dataName");
        if (!(dataName.get() instanceof DataName dataNameDN)) {
            throw new KException(ExceptionCode.KDE0012, "databaseName should be dataName");
        }


        if (!arguments.containsKey("session")){
            throw new KException(ExceptionCode.KDE0012,"Remove function need argument session");
        }
        Value<?> session = arguments.get("session");
        if (!(session.get() instanceof Session s )) {
            throw new KException(ExceptionCode.KDE0012, "session should be session type");
        }

        if(s.getSessionData().getVariables().containsKey(dataNameDN.getName())){
            s.getSessionData().getVariables().remove(dataNameDN.getName());
            return null;
        }
        else if(s.getSessionData().getSessionDataFrame().containsKey(dataNameDN.getName())){
            if(dataNameDN.getIndex() == null){
                s.getSessionData().deallocDataFrame(dataNameDN.getName());
                return null;
            }
            else {
                var columns = s.getSessionData().getSessionDataFrame().get(dataNameDN.getName());
                columns.removeColumn(dataNameDN.getIndex());
                return null;
            }
        }
        else {
            throw new KException(ExceptionCode.KDE0012, "variable not found");
        }

    }
}
