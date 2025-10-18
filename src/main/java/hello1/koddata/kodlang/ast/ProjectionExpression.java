package hello1.koddata.kodlang.ast;

public class ProjectionExpression extends Expression {

    final Expression proj;
    final Expression rename;
    public ProjectionExpression(Expression proj, Expression rename){
        super(StatementType.PROJECTION);
        this.proj = proj;
        this.rename = rename;
    }
}
