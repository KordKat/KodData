package hello1.koddata.engine;

import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.net.UserClient;
import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.SessionData;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class StatementExecutor {

    public static void executeStatement(Statement statement, UserClient client) throws KException {
        if (statement instanceof BlockStatement block) {
            for (Statement stmt : block.statements) {
                executeStatement(stmt, client);
            }
        } else if (statement instanceof Expression expression) {
            Value<?> result = evaluateExpression(expression, client);
            client.write(ByteBuffer.wrap(("- " + result.get().getClass().getName()).getBytes(StandardCharsets.UTF_8)));
        }
    }

    private static Value<?> evaluateExpression(Expression expression, UserClient client) throws KException {
        if (expression instanceof BinaryExpression e) {
            Value<?> leftVal = evaluateExpression(e.left, client);
            Value<?> rightVal = evaluateExpression(e.right, client);

            Object left = leftVal.get();
            Object right = rightVal.get();

            switch (e.op) {
                case ADD -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() + n2.doubleValue());
                    }
                    if (left instanceof String || right instanceof String) {
                        return new Value<>(String.valueOf(left) + String.valueOf(right));
                    }
                }
                case SUB -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() - n2.doubleValue());
                    }
                }
                case MUL -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() * n2.doubleValue());
                    }
                }
                case DIV -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        if (n2.doubleValue() == 0) return new NullValue("Division by zero");
                        return new Value<>(n1.doubleValue() / n2.doubleValue());
                    }
                }

                case POWER -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(Math.pow(n1.doubleValue(), n2.doubleValue()));
                    }
                }
                case AND -> {
                    if (left instanceof Boolean b1 && right instanceof Boolean b2) {
                        return new Value<>(b1 && b2);
                    }
                }

                case OR -> {
                    if (left instanceof Boolean b1 && right instanceof Boolean b2) {
                        return new Value<>(b1 || b2);
                    }
                }
                case EQUALS -> {
                    return new Value<>(Objects.equals(left, right));
                }

                case NEQUALS -> {
                    return new Value<>(!Objects.equals(left, right));
                }
                case GREATER -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() > n2.doubleValue());
                    }
                }
                case GREATEREQ -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() >= n2.doubleValue());
                    }
                }
                case LESSTHAN -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() < n2.doubleValue());
                    }
                }

                case LESSTHANEQ -> {
                    if (left instanceof Number n1 && right instanceof Number n2) {
                        return new Value<>(n1.doubleValue() <= n2.doubleValue());
                    }
                }
                case IN -> {
                    if (left instanceof String s1 && right instanceof String s2) {
                        return new Value<>(s2.contains(s1));
                    }
                    if (right instanceof List<?> list) {
                        return new Value<>(list.contains(left));
                    }
                }
            }
        } else if (expression instanceof NumberLiteral num) {
            double value = Double.parseDouble(new String(num.literal));
            return new Value<>(value);
        } else if (expression instanceof ArrayLiteral array) {
            List<?> values = Arrays.stream(array.literals.toArray())
                    .map(x -> {
                        try {
                            return evaluateExpression(x, client);
                        } catch (KException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toList();
            return new Value<>(values);
        } else if (expression instanceof AssignmentExpression assign) {
            Expression toExpr = assign.left;
            Expression valueExpr = assign.right;

            Value<?> result = evaluateExpression(valueExpr, client);

            if (toExpr instanceof Identifier i) {
                client.getCurrentSession()
                        .getSessionData()
                        .assignVariable(new DataName(i.identifier, null), result.get());
                return result;
            } else if (toExpr instanceof Subscript sub) {

                Expression baseExpr = sub.base;
                Expression indexExpr = sub.index;


                if (baseExpr instanceof Identifier id && indexExpr instanceof NIdentifier nid) {
                    client.getCurrentSession()
                            .getSessionData()
                            .assignVariable(new DataName(id.identifier, nid.identifier), result.get());
                    return result;
                }

                Value<?> baseValue = evaluateExpression(baseExpr, client);
                Value<?> subValue = evaluateExpression(indexExpr, client);

                if (baseValue.get() instanceof List<?> list && subValue.get() instanceof Integer idx) {

                    if (idx < 0 || idx >= list.size())
                        return new NullValue("Index out of bounds");

                    List<Object> newList = new ArrayList<>(list);
                    newList.set(idx, result.get());

                    return new Value<>(newList);
                }

                return new NullValue("Invalid subscript assignment");
            } else if (toExpr instanceof ArrayLiteral array) {

                if (!(result.get() instanceof List<?> list))
                    return new NullValue("Right-hand side must be a list");

                if (list.size() != array.literals.length())
                    return new NullValue("Array length mismatch");

                for (int i = 0; i < array.literals.length(); i++) {

                    Expression leftCurr = array.literals.get(i);
                    Object rhsObj = list.get(i);

                    Object actualValue = (rhsObj instanceof Value<?> v) ? v.get() : rhsObj;

                    if (leftCurr instanceof Identifier i2) {
                        client.getCurrentSession()
                                .getSessionData()
                                .assignVariable(new DataName(i2.identifier, null), actualValue);
                    } else if (leftCurr instanceof Subscript sub) {

                        Expression baseExpr = sub.base;
                        Expression indexExpr = sub.index;

                        if (baseExpr instanceof Identifier id && indexExpr instanceof NIdentifier nid) {
                            client.getCurrentSession()
                                    .getSessionData()
                                    .assignVariable(new DataName(id.identifier, nid.identifier), actualValue);
                        } else {
                            Value<?> baseValue = evaluateExpression(baseExpr, client);
                            Value<?> subValue = evaluateExpression(indexExpr, client);

                            if (baseValue.get() instanceof List<?> baseList &&
                                    subValue.get() instanceof Integer idx) {

                                if (idx < 0 || idx >= baseList.size())
                                    return new NullValue("Index out of bounds");

                                List<Object> updatedList = new ArrayList<>(baseList);
                                updatedList.set(idx, actualValue);

                                client.getCurrentSession()
                                        .getSessionData()
                                        .assignVariable(new DataName("_", null), updatedList);
                            } else {
                                return new NullValue("Invalid subscript in destructuring");
                            }
                        }
                    } else {
                        return new NullValue("Invalid destructuring target");
                    }
                }

                return result;
            }

            return new NullValue("Invalid assignment target");
        } else if (expression instanceof BooleanLiteral bool){
            return new Value<>(bool.literal);
        } else if (expression instanceof Identifier i){
            SessionData sessionData = client.getCurrentSession().getSessionData();

        } else if (expression instanceof NullLiteral n){
            return new NullValue("null");
        }else if(expression instanceof FunctionCall fc){ //string -> new Value<>("Task {id} started");

        } else if(expression instanceof Pipeline pipe){ //task id
            if(pipe instanceof BranchPipeline bp){

            }else {

            }
        }else if(expression instanceof BranchMember bm){
            //...
        }else if(expression instanceof StringLiteral str){
            return new Value<>(new String(str.literal));
        } else if(expression instanceof Subscript sub){
            
        }else if(expression instanceof UnaryExpression unary){

        }

        return new NullValue("Invalid");

    }
}