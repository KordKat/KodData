package hello1.koddata.kodlang.ast;

public class NumberLiteral extends Expression {

    final char[] literal;

    public NumberLiteral(char[] literal){
        super(StatementType.NUMBER_LITERAL);
        this.literal = literal;
    }

}
