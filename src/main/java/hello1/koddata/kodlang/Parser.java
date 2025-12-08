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

    public BlockStatement parseStatements() throws KException {
        List<Statement> statements = new ArrayList<>();
        while(current() != null && !current().type.equals(Token.TokenType.EOF)){
            statements.add(parseStatement());
        }
        return new BlockStatement(new ImmutableArray<>(statements));
    }

    public Statement parseStatement() throws KException {
        Statement stmt = parseExpression();
        expect(Token.TokenType.SEMICOLON);
        return stmt;
    }

    private BlockStatement parseBlockStatement() throws KException {
        List<Statement> statements = new ArrayList<>();
        while(current() != null &&
                !current().type.equals(Token.TokenType.RCURLY) &&
                !current().type.equals(Token.TokenType.EOF)){
            statements.add(parseStatement());
        }
        if(current() != null){
            consume();
        }
        return new BlockStatement(new ImmutableArray<>(statements));
    }

    private Expression parseExpression() throws KException {
        return parseAssignmentExpression();
    }

    private Expression parseAssignmentExpression() throws KException {
        Expression lhs = parseOrExpression();
        if(current() != null && current().type.equals(Token.TokenType.ASSIGN)){
            consume();
            Expression rhs = parseAssignmentExpression();
            return new AssignmentExpression(lhs, rhs);
        }
        return lhs;
    }

    private Expression parseOrExpression() throws KException {
        Expression lhs = parseAndExpression();
        while(current() != null && current().type.equals(Token.TokenType.OP_OR)){
            consume();
            Expression rhs = parseAndExpression();
            lhs = new BinaryExpression(BinaryExpression.Operator.OR, lhs, rhs);
        }
        return lhs;
    }

    private Expression parseAndExpression() throws KException {
        Expression lhs = parseEqualityExpression();
        while(current() != null && current().type.equals(Token.TokenType.OP_AND)){
            consume();
            Expression rhs = parseEqualityExpression();
            lhs = new BinaryExpression(BinaryExpression.Operator.AND, lhs, rhs);
        }
        return lhs;
    }

    private Expression parseEqualityExpression() throws KException {
        Expression lhs = parseComparisonExpression();
        while(current() != null){
            Token t = current();
            BinaryExpression.Operator op = switch (t.type){
                case OP_EQ -> BinaryExpression.Operator.EQUALS;
                case OP_NEQ -> BinaryExpression.Operator.NEQUALS;
                case OP_IN -> BinaryExpression.Operator.IN;
                default -> null;
            };
            if(op == null) break;
            consume();
            Expression rhs = parseComparisonExpression();
            lhs = new BinaryExpression(op, lhs, rhs);
        }
        return lhs;
    }

    private Expression parseComparisonExpression() throws KException {
        Expression lhs = parseAddSubExpression();
        while(current() != null){
            Token t = current();
            BinaryExpression.Operator op = switch (t.type){
                case OP_GT -> BinaryExpression.Operator.GREATER;
                case OP_GE -> BinaryExpression.Operator.GREATEREQ;
                case OP_LT -> BinaryExpression.Operator.LESSTHAN;
                case OP_LE -> BinaryExpression.Operator.LESSTHANEQ;
                default -> null;
            };
            if(op == null) break;
            consume();
            Expression rhs = parseAddSubExpression();
            lhs = new BinaryExpression(op, lhs, rhs);
        }
        return lhs;
    }

    private Expression parseAddSubExpression() throws KException {
        Expression lhs = parseMulDivExpression();
        while(current() != null){
            Token t = current();
            if(!t.type.equals(Token.TokenType.OP_ADD) &&
                    !t.type.equals(Token.TokenType.OP_SUB)){
                break;
            }
            consume();
            Expression rhs = parseMulDivExpression();
            BinaryExpression.Operator op = t.type.equals(Token.TokenType.OP_ADD)
                    ? BinaryExpression.Operator.ADD
                    : BinaryExpression.Operator.SUB;
            lhs = new BinaryExpression(op, lhs, rhs);
        }
        return lhs;
    }

    private Expression parseMulDivExpression() throws KException {
        Expression lhs = parsePowerExpression();
        while(current() != null){
            Token t = current();
            if(!t.type.equals(Token.TokenType.OP_MUL) &&
                    !t.type.equals(Token.TokenType.OP_DIV)){
                break;
            }
            consume();
            Expression rhs = parsePowerExpression();
            BinaryExpression.Operator op = t.type.equals(Token.TokenType.OP_MUL)
                    ? BinaryExpression.Operator.MUL
                    : BinaryExpression.Operator.DIV;
            lhs = new BinaryExpression(op, lhs, rhs);
        }
        return lhs;
    }

    private Expression parsePowerExpression() throws KException {
        Expression lhs = parseUnaryExpression();
        if(current() != null && current().type.equals(Token.TokenType.OP_POW)){
            consume();
            Expression rhs = parsePowerExpression();
            return new BinaryExpression(BinaryExpression.Operator.POWER, lhs, rhs);
        }
        return lhs;
    }

    private Expression parseUnaryExpression() throws KException {
        if(current() != null && current().type.equals(Token.TokenType.OP_SUB)){
            consume();
            return new UnaryExpression(BinaryExpression.Operator.SUB, parseUnaryExpression());
        }
        return parseCallExpression();
    }

    private Expression parseCallExpression() throws KException {
        Expression expr = parsePrimaryExpression();

        if(expr instanceof NIdentifier nident){
            String funcName = nident.identifier;

            if(isFunction(funcName)){
                expr = parseFunctionArguments(nident);
            }
        }

        while(current() != null){
            Token next = current();

            if(next.type.equals(Token.TokenType.LBRACKET)){
                consume();
                Expression indexExpr = parseExpression();
                expect(Token.TokenType.RBRACKET);
                expr = new Subscript(expr, indexExpr);
            }
            else {
                break;
            }
        }

        return expr;
    }

    private Expression parseFunctionArguments(NIdentifier identifier) throws KException {
        List<Expression> arguments = new ArrayList<>();

        if(current() != null && canStartExpression(current().type)){
            arguments.add(parseExpression());

            while(current() != null && current().type.equals(Token.TokenType.COMMA)){
                consume();

                if(current() == null || !canStartExpression(current().type)){
                    throw new KException(ExceptionCode.KDC0002,
                            "Expected argument after comma in function call");
                }

                arguments.add(parseExpression());
            }
        }

        return new FunctionCall(identifier, new ImmutableArray<>(arguments));
    }

    private boolean canStartExpression(Token.TokenType type){
        return type.equals(Token.TokenType.NUMBER) ||
                type.equals(Token.TokenType.STRING) ||
                type.equals(Token.TokenType.IDENTIFIER) ||
                type.equals(Token.TokenType.NIDENTIFIER) ||
                type.equals(Token.TokenType.LPAREN) ||
                type.equals(Token.TokenType.LBRACKET) ||
                type.equals(Token.TokenType.PIPELINE) ||
                type.equals(Token.TokenType.BRANCH) ||
                type.equals(Token.TokenType.OP_SUB) ||
                type.equals(Token.TokenType.NULL) ||
                type.equals(Token.TokenType.TRUE) ||
                type.equals(Token.TokenType.FALSE);
    }

    private Expression parsePrimaryExpression() throws KException {
        Token t = current();

        if (t == null || t.type.equals(Token.TokenType.EOF)) {
            throw new KException(ExceptionCode.KDC0002, "Unexpected end of input");
        }

        Expression expr = null;

        switch (t.type) {
            case NUMBER -> {
                String s = new String(t.lexeme);
                consume();
                if (current() != null && current().type.equals(Token.TokenType.DOT)) {
                    Token peek = peek(1);
                    if(peek != null && peek.type.equals(Token.TokenType.NUMBER)){
                        consume();
                        Token next = current();
                        consume();
                        s += "." + new String(next.lexeme);
                        expr = new NumberLiteral(s.toCharArray(), true);
                    } else {
                        expr = new NumberLiteral(s.toCharArray(), false);
                    }
                } else {
                    expr = new NumberLiteral(s.toCharArray(), false);
                }
            }
            case STRING -> {
                consume();
                expr = new StringLiteral(t.lexeme);
            }
            case NULL -> {
                consume();
                expr = new NullLiteral();
            }
            case LPAREN -> {
                consume();
                Expression inner = parseExpression();
                expect(Token.TokenType.RPAREN);
                expr = inner;
            }
            case TRUE -> {
                consume();
                expr = new BooleanLiteral(true);
            }
            case FALSE -> {
                consume();
                expr = new BooleanLiteral(false);
            }
            case LBRACKET -> {
                consume();
                List<Expression> list = new ArrayList<>();

                while (current() != null &&
                        !current().type.equals(Token.TokenType.RBRACKET) &&
                        !current().type.equals(Token.TokenType.EOF)) {

                    list.add(parseExpression());

                    if(current() != null && current().type.equals(Token.TokenType.COMMA)){
                        consume();
                        if(current() != null && current().type.equals(Token.TokenType.RBRACKET)){
                            break;
                        }
                    } else {
                        break;
                    }
                }

                expect(Token.TokenType.RBRACKET);
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
                consume();
                expect(Token.TokenType.LCURLY);
                List<Expression> pipeline = new ArrayList<>();

                while(current() != null &&
                        !current().type.equals(Token.TokenType.EOF) &&
                        !current().type.equals(Token.TokenType.RCURLY)){
                    pipeline.add(parseExpression());
                    expect(Token.TokenType.SEMICOLON);
                }

                expect(Token.TokenType.RCURLY);
                expr = new Pipeline(new ImmutableArray<>(pipeline));
            }
            case BRANCH -> {
                consume();
                expect(Token.TokenType.LCURLY);
                List<WhenCaseStatement> whens = new ArrayList<>();
                ElseCaseStatement elseCase = null;

                while(current() != null && current().type.equals(Token.TokenType.WHEN)){
                    consume();
                    whens.add(parseWhenCase());
                }

                if(current() != null && current().type.equals(Token.TokenType.ELSE)){
                    consume();
                    elseCase = new ElseCaseStatement(parseExpression());
                }

                expect(Token.TokenType.RCURLY);
                expr = new BranchPipeline(new ImmutableArray<>(whens), elseCase);
            }
            default -> throw new KException(ExceptionCode.KDC0002,
                    "Unexpected token in expression: " + t.type);
        }

        return expr;
    }

    private WhenCaseStatement parseWhenCase() throws KException {
        Expression condition = parseExpression();

        if(current() == null || !current().type.equals(Token.TokenType.DO)){
            throw new KException(ExceptionCode.KDC0002, "Expected token 'DO' in when case");
        }
        consume();

        Expression doPipe = parseExpression();

        if(current() != null && current().type.equals(Token.TokenType.SEMICOLON)){
            consume();
        }

        return new WhenCaseStatement(condition, doPipe);
    }

    private void consume(){
        Token peek = peek(0);
        if(peek != null) {
            System.out.println(peek.type);
        }
        position++;
    }

    private Token peek(int offset){
        int pos = position + offset;
        if(pos >= tokens.length()) return null;
        return tokens.get(pos);
    }

    private static boolean isFunction(String name){
        Set<String> functionName = Set.of(
                "max", "min", "abs", "sqrt", "pow", "exp", "log", "log10",
                "sin", "cos", "tan", "asin", "acos", "atan", "ceil", "floor",
                "round", "clamp", "random", "sign", "mod", "sinh",
                "cosh", "tanh", "deg", "rad", "gcd", "lcm", "factorial", "root",
                "sum", "avg", "mean", "median", "mode", "count",
                 "range", "product",
                "equals", "not", "and", "or", "xor", "if", "ifelse",
                "length", "upper", "lower", "trim", "concat", "substring",
                "replace", "indexof", "startswith", "endswith", "split", "join",
                "reverse", "contains", "padleft", "padright", "repeat",
                "tostring", "str", "format", "match", "regex",
                "sort",
                "map", "filter", "reduce", "first", "last", "distinct", "every",
                "some", "any", "merge",
                "type", "isnumber", "isstring", "islist", "isbool", "print",
                "coalesce", "cast",
                "take", "skip", "fill"
                ,"connect","download", "export", "fetch", "remove", "session", "task"
                ,"user", "copy", "drop", "apply", "stop"
        );
        return functionName.contains(name.toLowerCase());
    }

    private void expect(Token.TokenType type) throws KException {
        Token curr = current();
        if(curr == null || !curr.type.equals(type)){
            throw new KException(ExceptionCode.KDC0002,
                    "Expected " + type + " but got " + (curr == null ? "EOF" : curr.type));
        }
        consume();
    }

    private Token current(){
        if (position >= tokens.length()) return null;
        return tokens.get(position);
    }
}