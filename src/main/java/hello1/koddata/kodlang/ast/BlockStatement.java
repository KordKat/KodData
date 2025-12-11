package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

//Inheritance
public class BlockStatement extends Statement {

    public final ImmutableArray<Statement> statements;

    public BlockStatement(ImmutableArray<Statement> statements) {
        super(StatementType.BLOCK);
        this.statements = statements;
    }
}
