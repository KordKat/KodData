package hello1.koddata.kodlang.ast;

public class FetchExpression extends Expression {

    public enum DataSource {
        CSV,
        JSON,
        DATABASE,
        PSV
    }

    final DataSource dataSource;
    final Expression fetchSource;
    final Expression queryString;

    public FetchExpression(DataSource source, Expression fetchSource, Expression queryString){
        super(StatementType.FETCH);
        this.dataSource = source;
        this.fetchSource = fetchSource;
        this.queryString = queryString;
    }

}
