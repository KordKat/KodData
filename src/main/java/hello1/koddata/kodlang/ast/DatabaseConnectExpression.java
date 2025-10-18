package hello1.koddata.kodlang.ast;

public class DatabaseConnectExpression extends Expression {

    final StringLiteral databaseT, databaseName, user, pass;
    final NumberLiteral port;

    public DatabaseConnectExpression(StringLiteral databaseT, StringLiteral databaseName, StringLiteral user, StringLiteral pass, NumberLiteral port) {
        super(StatementType.CONNECT);
        this.databaseT = databaseT;
        this.databaseName = databaseName;
        this.user = user;
        this.pass = pass;
        this.port = port;
    }
}
