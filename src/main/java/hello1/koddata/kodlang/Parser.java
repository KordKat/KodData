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
        Expression lhs = parseLHS();

        if(current().type.equals(Token.TokenType.ASSIGN)){
            consume();
            Expression rhs = parseExpression();
            return new AssignmentExpression(lhs, rhs);
        }

        return lhs;
    }

    private Expression parseLHS() throws KException {
        Expression target = parseOrExpression();

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

    private Expression parsePrimaryExpression() {
        return null; //ขี้เกียจ
    }

    private void consume(){
        position++;
    }

    private static boolean isFunction(String name){
        Set<String> functionName = Set.of("std", "min");

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

}
