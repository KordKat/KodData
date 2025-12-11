package hello1.koddata.kodlang.ast;

//Inheritance
public non-sealed class WhenCaseStatement extends BranchMember {

    public final Expression condition;
    public final Expression doPipe;


    public WhenCaseStatement(Expression condition, Expression doPipe){
        this.condition = condition;
        this.doPipe = doPipe;
    }

}
