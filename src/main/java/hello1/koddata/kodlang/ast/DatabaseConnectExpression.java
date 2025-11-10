package hello1.koddata.kodlang.ast;

public class DatabaseConnectExpression extends Expression {

    final StringLiteral databaseName, user, pass, host;
    final NIdentifier databaseT;
    final NumberLiteral port;

    public DatabaseConnectExpression(NIdentifier databaseT, StringLiteral databaseName, StringLiteral user, StringLiteral pass, NumberLiteral port , StringLiteral host) {
        super(StatementType.CONNECT);
        this.databaseT = databaseT;
        this.databaseName = databaseName;
        this.user = user;
        this.pass = pass;
        this.port = port;
        this.host = host;
    }
}
