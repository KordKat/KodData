package hello1.koddata.kodlang.ast;

public class StringLiteral extends Expression {

    final char[] literal;

    public StringLiteral(char[] literal){
        super(StatementType.STRING_LITERAL);
        this.literal = literal;
    }

}
