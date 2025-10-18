package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class SelectStatement extends Expression {

    final ImmutableArray<ProjectionExpression> projection;
    final DataFrameDeclaration fromSource;
    final ImmutableArray<JoinExpression> joinExpressions;
    final Expression whereClause;
    final Expression groupByClause;
    final Expression havingClause;

    public SelectStatement(ImmutableArray<ProjectionExpression> projection, DataFrameDeclaration fromSource, ImmutableArray<JoinExpression> joinExpressions, Expression whereClause, Expression groupByClause, Expression havingClause){
        super(StatementType.SELECT);
        this.projection = projection;
        this.fromSource = fromSource;
        this.joinExpressions = joinExpressions;
        this.whereClause = whereClause;
        this.groupByClause = groupByClause;
        this.havingClause = havingClause;
    }
}
