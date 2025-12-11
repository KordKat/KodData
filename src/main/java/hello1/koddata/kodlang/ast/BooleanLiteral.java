package hello1.koddata.kodlang.ast;

//Inheritance
public class BooleanLiteral extends Expression {

    public boolean literal;

    public BooleanLiteral(boolean literal){
        super(StatementType.BOOLEAN_LITERAL);
        this.literal = literal;
    }

}
