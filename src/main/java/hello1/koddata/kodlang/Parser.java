package hello1.koddata.kodlang;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.ArrayList;
import java.util.List;

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
        Expression lhs = parseLHS();

        if(current().type.equals(Token.TokenType.ASSIGN)){
            consume();
            Expression rhs = parseExpression();
            return new AssignmentExpression(lhs, rhs);
        }

        return lhs;
    }

    private Expression parseLHS() throws KException {
        Expression target = parseConditionalExpression();

        while(true){
            Token t = current();
            if(t.type.equals(Token.TokenType.DOT)){
                expect(Token.TokenType.IDENTIFIER);
                target = new PropertyAccessExpression(target, new Identifier(new String(current().lexeme)));
                consume();
            }else if(t.type.equals(Token.TokenType.LBRACKET)){
                consume();
                Expression index = parseExpression();
                expect(Token.TokenType.RBRACKET);
                consume();
                target = new Subscript(target, index);
            }else {
                break;
            }
        }

        return target;
    }

    private Expression parseConditionalExpression(){
        return null;
    }
    private void consume(){
        position++;
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

}
