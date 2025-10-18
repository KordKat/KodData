package hello1.koddata.kodlang.ast;

public abstract class Statement {

    public enum StatementType {
        BLOCK,
        ASSIGNMENT,
        APPLY,
        DELETE,
        DOWNLOAD,

        BINARY_EXPR,
        SELECT,
        NULL_LITERAL,
        BOOLEAN_LITERAL,
        NUMBER_LITERAL,
        STRING_LITERAL,
        ARRAY_LITERAL,
        IDENTIFIER,
        SUBSCRIPT,
        FETCH,
        CONNECT,
        BRANCH,
        BRANCH_MEMBER,
        PIPELINE_FUNC,
        PROJECTION,
        JOIN,
        DATAFRAME
    }

    protected StatementType type;

    public Statement(StatementType type){
        this.type = type;
    }

}
