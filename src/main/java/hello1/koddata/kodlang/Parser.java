package hello1.koddata.kodlang;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Parser {

    private final ImmutableArray<Token> tokens;
    private int position = 0;

    public Parser(ImmutableArray<Token> tokens){
        this.tokens = tokens;
    }

    public Statement parseStatement() throws KException {
        final Token first = current();
        consume();
        Token t = current();
        if(first.type.equals(Token.TokenType.DELETE)){
            return parseDeleteStatement();
        }else if(first.type.equals(Token.TokenType.DOWNLOAD)){
            return parseDownloadStatement();
        }else if(first.type.equals(Token.TokenType.LCURLY)){
            return parseBlockStatement();
        }else if(first.type.equals(Token.TokenType.APPLY)){
            return parseApplyStatement();
        }else {
            return parseExpression();
        }
    }

    public BlockStatement parseStatements() throws KException {
        List<Statement> statements = new ArrayList<>();
        while(current() != null && !current().type.equals(Token.TokenType.EOF)){
            statements.add(parseStatement());
        }
        return new BlockStatement(new ImmutableArray<>(statements));
    }
    // DELETE $expr;
    private DeleteStatement parseDeleteStatement() throws KException {
        Expression expr = parseExpression();
        return new DeleteStatement(expr);
    }

    /*
    * {
    *   $statements
    * }
    * */
    private BlockStatement parseBlockStatement() throws KException {
        List<Statement> statements = new ArrayList<>();
        while(current() != null && !current().type.equals(Token.TokenType.RCURLY) && !current().type.equals(Token.TokenType.EOF)){
            statements.add(parseStatement());
        }
        if(current() != null){
            consume();
        }
        return new BlockStatement(new ImmutableArray<>(statements));
    }

    // DOWNLOAD $expr;
    private DownloadStatement parseDownloadStatement() throws KException {
        Expression expr = parseExpression();
        return new DownloadStatement(expr);
    }

    //APPLY $pipe TO $expr;
    private ApplyStatement parseApplyStatement() throws KException {
        Expression pipe = parseExpression();
        expect(Token.TokenType.TO);
        consume();
        Expression expr = parseExpression();
        return new ApplyStatement(pipe, expr);
    }

    private Expression parseExpression() throws KException {
        //TODO: parsing expression
        return parseAssignmentExpression();
    }

    private Expression parseAssignmentExpression() throws KException {
        Expression lhs = parseOrExpression();

        if(current().type.equals(Token.TokenType.ASSIGN)){
            consume();
            Expression rhs = parseAssignmentExpression();
            return new AssignmentExpression(lhs, rhs);
        }

        return lhs;
    }

    private Expression parseOrExpression() throws KException {
        Expression lhs = parseAndExpression();

        Token t = current();
        if(t.type.equals(Token.TokenType.OP_OR)){
            consume();
            Expression rhs = parseOrExpression();
            return new BinaryExpression(BinaryExpression.Operator.OR, lhs, rhs);
        }

        return lhs;
    }

    private Expression parseAndExpression() throws KException {
        Expression lhs = parseEqualityExpression();

        Token t = current();
        if(t.type.equals(Token.TokenType.OP_AND)){
            consume();
            Expression rhs = parseAndExpression();
            return new BinaryExpression(BinaryExpression.Operator.AND, lhs, rhs);
        }

        return lhs;
    }

    private Expression parseEqualityExpression() throws KException {
        Expression lhs = parseComparisonExpression();

        Token t = current();

        if(t.type.equals(Token.TokenType.OP_EQ) || t.type.equals(Token.TokenType.OP_NEQ) || t.type.equals(Token.TokenType.OP_IN)){
            consume();
            Expression rhs = parseEqualityExpression();
            BinaryExpression.Operator op = switch (t.type){
                case OP_EQ -> BinaryExpression.Operator.EQUALS;
                case OP_NEQ -> BinaryExpression.Operator.NEQUALS;
                case OP_IN -> BinaryExpression.Operator.IN;
                default -> null;
            };
            return new BinaryExpression(op, lhs, rhs);
        }

        return lhs;
    }

    private Expression parseComparisonExpression() throws KException {
        Expression lhs = parseAddSubExpression();

        Token t = current();
        BinaryExpression.Operator op = switch (t.type){
            case OP_GT -> BinaryExpression.Operator.GREATER;
            case OP_GE -> BinaryExpression.Operator.GREATEREQ;
            case OP_LT -> BinaryExpression.Operator.LESSTHAN;
            case OP_LE -> BinaryExpression.Operator.LESSTHANEQ;
            default -> null;
        };
        if(op != null){
            consume();
            Expression rhs = parseComparisonExpression();
            return new BinaryExpression(op, lhs, rhs);
        }

        return lhs;
    }

    private Expression parseAddSubExpression() throws KException {
        Expression lhs = parseMulDivExpression();

        Token t = current();
        if(t.type.equals(Token.TokenType.OP_ADD) || t.type.equals(Token.TokenType.OP_SUB)){
            consume();
            Expression rhs = parseAddSubExpression();
            if(t.type.equals(Token.TokenType.OP_ADD)){
                return new BinaryExpression(BinaryExpression.Operator.ADD, lhs, rhs);
            }else if(t.type.equals(Token.TokenType.OP_SUB)){
                return new BinaryExpression(BinaryExpression.Operator.SUB, lhs, rhs);
            }
        }
        return lhs;
    }

    private Expression parseMulDivExpression() throws KException {
        Expression lhs = parsePowerExpression();

        Token t = current();
        if(t.type.equals(Token.TokenType.OP_MUL) || t.type.equals(Token.TokenType.OP_DIV)){
            consume();
            Expression rhs = parseMulDivExpression();
            if(t.type.equals(Token.TokenType.OP_MUL)){
                return new BinaryExpression(BinaryExpression.Operator.MUL, lhs, rhs);
            }else if(t.type.equals(Token.TokenType.OP_DIV)){
                return new BinaryExpression(BinaryExpression.Operator.DIV, lhs, rhs);
            }
        }
        return lhs;
    }

    private Expression parsePowerExpression() throws KException {
        Expression lhs = parseNIdentifierExpression();

        Token t = current();
        if(t.type.equals(Token.TokenType.OP_AND)){
            consume();
            Expression rhs = parsePowerExpression();
            return new BinaryExpression(BinaryExpression.Operator.POWER, lhs, rhs);
        }

        return lhs;
    }

    private Expression parseNIdentifierExpression() throws KException {
        Expression lhs = parsePrimaryExpression();

        if(lhs instanceof NIdentifier identifier && isFunction(identifier.identifier)){
            List<Expression> arguments = new ArrayList<>();
            while(current() != null && !current().type.equals(Token.TokenType.EOF) && !current().type.equals(Token.TokenType.SEMICOLON)){
                arguments.add(parseExpression());
            }
            return new FunctionCall(identifier, new ImmutableArray<>(arguments));
        }

        return lhs;
    }

    private Expression parsePrimaryExpression() throws KException {
        Token t = current();
        Expression expr = null;

        switch (t.type) {
            case NUMBER -> {
                String s = new String(t.lexeme);
                consume();
                boolean isFloat = false;
                if (current().type.equals(Token.TokenType.DOT)) {
                    consume();
                    Token next = current();
                    expect(Token.TokenType.NUMBER);
                    s += "." + new String(next.lexeme);
                    isFloat = true;
                    consume();
                }
                expr = new NumberLiteral(s.toCharArray(), isFloat);
            }
            case STRING -> {
                consume();
                expr = new StringLiteral(t.lexeme);
            }
            case OP_SUB -> {
                consume();
                expr = new UnaryExpression(BinaryExpression.Operator.SUB, parseExpression());
            }
            case LPAREN -> {
                consume();
                Expression inner = parseExpression();
                expect(Token.TokenType.RPAREN);
                consume();
                expr = inner;
            }
            case LBRACKET -> { // list or array literal
                consume();
                List<Expression> list = new ArrayList<>();
                while (!current().type.equals(Token.TokenType.RBRACKET)) {
                    list.add(parseExpression());
                    if (current().type.equals(Token.TokenType.COMMA)) consume();
                    else break;
                }
                expect(Token.TokenType.RBRACKET);
                consume();
                expr = new ArrayLiteral(new ImmutableArray<>(list));
            }
            case IDENTIFIER -> {
                consume();
                expr = new Identifier(new String(t.lexeme));
            }
            case NIDENTIFIER -> {
                consume();
                expr = new NIdentifier(new String(t.lexeme));
            }
            case PIPELINE -> {
                expect(Token.TokenType.LCURLY);
                consume();
                List<Expression> pipeline = new ArrayList<>();
                while(!current().type.equals(Token.TokenType.EOF) && !current().type.equals(Token.TokenType.RCURLY)){
                    pipeline.add(parseExpression());
                }
                expect(Token.TokenType.RCURLY);
                consume();
                expr = new Pipeline(new ImmutableArray<>(pipeline));
            }
            case BRANCH -> {
                expect(Token.TokenType.LCURLY);
                consume();
                List<WhenCaseStatement> whens = new ArrayList<>();
                ElseCaseStatement elseCase = null;
                while(current().type.equals(Token.TokenType.WHEN)){
                    consume();
                    whens.add(parseWhenCase());
                }
                if(current().type.equals(Token.TokenType.ELSE)){
                    consume();
                    elseCase = new ElseCaseStatement(parseExpression());
                }
                expect(Token.TokenType.RCURLY);
                consume();
                expr = new BranchPipeline(new ImmutableArray<>(whens), elseCase);
            }
            default -> throw new KException(ExceptionCode.KDC0002, "Unexpected token in expression: " + t.type);
        }

        while (true) {
            Token next = current();
            if (next.type.equals(Token.TokenType.DOT)) {
                consume();
                Token prop = current();
                if (!prop.type.equals(Token.TokenType.IDENTIFIER)) {
                    throw new KException(ExceptionCode.KDC0002, "Expected identifier after '.' at " + prop.start);
                }
                expr = new PropertyAccessExpression(expr, new Identifier(new String(prop.lexeme)));
                consume();
            } else if (next.type.equals(Token.TokenType.LBRACKET)) {
                consume();
                Expression indexExpr = parseExpression();
                expect(Token.TokenType.RBRACKET);
                consume();
                expr = new Subscript(expr, indexExpr);
            } else {
                break;
            }
        }

        return expr;
    }

    private WhenCaseStatement parseWhenCase() throws KException {
        Expression condition = parseExpression();
        if(!current().type.equals(Token.TokenType.DO)){
            throw new KException(ExceptionCode.KDC0002, "Expected token 'DO' but got " + current().type + " instead.");
        }
        Expression doPipe = parseExpression();
        return new WhenCaseStatement(condition, doPipe);
    }


    private void consume(){
        position++;
    }

    private static boolean isFunction(String name){
        Set<String> functionName = Set.of("max", "min");

        return functionName.contains(name.toLowerCase());
    }

    private void expect(Token.TokenType type) throws KException {
        consume();
        Token curr = current();
        if(!curr.type.equals(type)){
            throw new KException(ExceptionCode.KDC0002, "Expected " + type + " got " + curr.type + " at " + curr.start + " - " + curr.end);
        }
    }

    private Token current(){
        return tokens.get(position);
    }

    private Token peek(int offset){
        if(position + offset < tokens.length()) {
            return tokens.get(position + offset);
        }else {
            return tokens.get(tokens.length() - 1);
        }
    }
//    -------------------------------------------------------
    private boolean isAtEnd() {
        Token t = current();
        return t == null || t.type.equals(Token.TokenType.EOF);
    }

    private boolean match(Token.TokenType... types) {
        for (Token.TokenType type : types) {
            if (current().type.equals(type)) {
                consume();
                return true;
            }
        }
        return false;
    }

    private Token previous() {
        return tokens.get(Math.max(0, position - 1));
    }

    private void synchronize() {
        while (!isAtEnd()) {
            if (previous().type.equals(Token.TokenType.SEMICOLON)) return;

            switch (current().type) {
                case DELETE, DOWNLOAD, APPLY, LCURLY -> { return; }
            }

            consume();
        }
    }

    private Token.TokenType peekType(int offset) {
        return peek(offset).type;
    }

}
