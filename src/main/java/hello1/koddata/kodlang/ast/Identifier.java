package hello1.koddata.kodlang.ast;

public class Identifier extends Expression {

    public final String identifier;

    public Identifier(String identifier){
        super(StatementType.IDENTIFIER);
        this.identifier = identifier;
    }

}
