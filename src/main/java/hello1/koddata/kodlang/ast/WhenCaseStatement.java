package hello1.koddata.kodlang.ast;

public non-sealed class WhenCaseStatement extends BranchMember {

    final Expression condition;
    final Expression doPipe;


    public WhenCaseStatement(Expression condition, Expression doPipe){
        this.condition = condition;
        this.doPipe = doPipe;
    }

}
