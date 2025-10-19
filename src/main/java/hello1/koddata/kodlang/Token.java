package hello1.koddata.kodlang;

public class Token {

    enum TokenType {
        IDENTIFIER,
        NIDENTIFIER,
        NUMBER,
        DOT,
        STRING,
        OP_ADD,
        OP_SUB,
        OP_MUL,
        OP_POW,
        OP_DIV,
        OP_NEQ,
        OP_EQ,
        OP_GT,
        OP_GE,
        OP_LT,
        OP_LE,
        OP_IN,
        OP_BETWEEN,
        OP_AND,
        OP_OR,
        LPAREN,
        RPAREN,
        LCURLY,
        RCURLY,
        LBRACKET,
        RBRACKET,
        ASSIGN,
        DELETE,
        FETCH,
        TO,
        FROM,
        CONNECT,
        AS,
        DATABASE,
        CSV,
        PSV,
        JSON,
        WITH,
        USING,
        GET,
        EXPORT,
        DOWNLOAD,
        OVER,
        WHEN,
        ELSE,
        SEMICOLON,
        BRANCH,
        PIPELINE,
        SELECT,
        WHERE,
        ORDER,
        BY,
        LIMIT,
        GROUP,
        HAVING,
        INNER,
        OUTER,
        LEFT,
        RIGHT,
        NATURAL,
        JOIN,
        EOF,
        NULL,
        TRUE,
        FALSE,
        APPLY,
        COMMA
    }

    TokenType type;
    char[] lexeme;
    int start, end;

    Token(TokenType type, char[] lexeme, int start, int end){
        this.type = type;
        this.lexeme = lexeme;
        this.start = start;
        this.end = end;
    }

    public String toString(){
        return String.format("Token(%s, \"%s\")", type, new String(lexeme));

    }

}
