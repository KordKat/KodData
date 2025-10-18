package hello1.koddata.kodlang.ast;

public class BooleanLiteral extends Expression {

    boolean literal;

    public BooleanLiteral(boolean literal){
        super(StatementType.BOOLEAN_LITERAL);
        this.literal = literal;
    }

}
