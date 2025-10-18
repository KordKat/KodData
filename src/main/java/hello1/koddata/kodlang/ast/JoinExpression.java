package hello1.koddata.kodlang.ast;

public class JoinExpression extends Expression {

    public enum JoinType {
        INNER,
        OUTER,
        LEFT,
        RIGHT,
        NATURAL
    }
    final JoinType joinType;
    final DataFrameDeclaration df;
    final Expression leftOn;
    final Expression rightOn;
    public JoinExpression(JoinType joinType, DataFrameDeclaration df, Expression leftOn, Expression rightOn){
        super(StatementType.JOIN);
        this.joinType = joinType;
        this.df = df;
        this.leftOn = leftOn;
        this.rightOn = rightOn;
    }

}
