package hello1.koddata.kodlang.ast;

public class ApplyStatement extends Statement {

    final Expression pipeline;
    final Expression expression;

    public ApplyStatement(Expression pipeline, Expression expression){
        super(StatementType.APPLY);
        this.pipeline = pipeline;
        this.expression = expression;
    }

}
