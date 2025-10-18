package hello1.koddata.kodlang.ast;

public class ApplyStatement extends Statement {

    final Pipeline pipeline;
    final Expression expression;

    public ApplyStatement(Pipeline pipeline, Expression expression){
        super(StatementType.APPLY);
        this.pipeline = pipeline;
        this.expression = expression;
    }

}
