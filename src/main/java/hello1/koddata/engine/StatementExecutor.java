package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.engine.function.*;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.net.UserClient;
import hello1.koddata.sessions.SessionData;
import hello1.koddata.utils.collection.ImmutableArray;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

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
        } else if (expression instanceof Identifier i) {
            SessionData sessionData = client.getCurrentSession().getSessionData();
            String name = i.identifier;

            Value<?> variableValue = sessionData.get(name);
            if (variableValue != null) {
                return variableValue;
            }

            Map<String, ColumnArray> dataFrames = sessionData.getSessionDataFrame();
            if (dataFrames.containsKey(name)) {
                return new Value<>(dataFrames.get(name));
            }

            throw new KException(ExceptionCode.KDC0001, "Variable or DataFrame '" + name + "' not defined");

        } else if (expression instanceof NullLiteral n){
            return new NullValue("null");
        }else if(expression instanceof FunctionCall fc){ //string -> new Value<>("Task {id} started");
            // 1. Get function name and evaluate arguments
            String functionName = fc.function.identifier;
            List<Value<?>> evaluatedArguments = new ArrayList<>();
            for (Expression argExpr : fc.arguments.toArray()) {
                Value<?> argValue = evaluateExpression(argExpr, client);
                // Check for null result during argument evaluation
                if (argValue instanceof NullValue) {
                    throw new KException(ExceptionCode.KDC0001, "argValue cant be null");
                }
                evaluatedArguments.add(argValue);
            }

            switch (functionName) {
                // 🔢 คณิตศาสตร์
                case "max":
                    return new Value<>(new QueryOperationNode(new MaxOperation() , evaluatedArguments));

                case "min":
                    return new Value<>(new QueryOperationNode(new MinOperation() , evaluatedArguments));

                case "abs":
                    return new Value<>(new QueryOperationNode(new AbsOperation() , evaluatedArguments));
                case "sqrt":
                    return new Value<>(new QueryOperationNode(new SqrtOperation() , evaluatedArguments));
                case "pow":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new PowOperation(n.doubleValue()) , evaluatedArguments));
                case "exp":
                    return new Value<>(new QueryOperationNode(new ExpOperation() , evaluatedArguments));
                case "log":
                    return new Value<>(new QueryOperationNode(new LogOperation() , evaluatedArguments));
                case "log10":
                    return new Value<>(new QueryOperationNode(new Log10Operation() , evaluatedArguments));
                case "sin":
                    return new Value<>(new QueryOperationNode(new SinOperation() , evaluatedArguments));
                case "cos":
                    return new Value<>(new QueryOperationNode(new CosOperation() , evaluatedArguments));
                case "tan":
                    return new Value<>(new QueryOperationNode(new TanOperation() , evaluatedArguments));
                case "asin":
                    return new Value<>(new QueryOperationNode(new ASinOperation() , evaluatedArguments));
                case "acos":
                    return new Value<>(new QueryOperationNode(new ACosOperation() , evaluatedArguments));
                case "atan":
                    return new Value<>(new QueryOperationNode(new ATanOperation() , evaluatedArguments));
                case "ceil":
                    return new Value<>(new QueryOperationNode(new CeilOperation() , evaluatedArguments));
                case "floor":
                    return new Value<>(new QueryOperationNode(new FloorOperation() , evaluatedArguments));
                case "round":
                    return new Value<>(new QueryOperationNode(new RoundOperation() , evaluatedArguments));
                case "clamp":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    if (!(evaluatedArguments.get(1).get() instanceof Number n2)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new ClampOperation(n1.doubleValue() , n2.doubleValue()) , evaluatedArguments));
                case "sign":
                    return new Value<>(new QueryOperationNode(new SignOperation() , evaluatedArguments));
                case "mod":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new ModOperation(n.doubleValue()) , evaluatedArguments));
                case "sinh":
                    return new Value<>(new QueryOperationNode(new SinhOperation() , evaluatedArguments));
                case "cosh":
                    return new Value<>(new QueryOperationNode(new CoshOperation() , evaluatedArguments));
                case "tanh":
                    return new Value<>(new QueryOperationNode(new TanhOperation() , evaluatedArguments));
                case "deg":
                    return new Value<>(new QueryOperationNode(new DegOperation() , evaluatedArguments));
                case "rad":
                    return new Value<>(new QueryOperationNode(new RadOperation() , evaluatedArguments));
                case "factorial":
                    return new Value<>(new QueryOperationNode(new FactorialOperation() , evaluatedArguments));
                case "root":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new RootOperation(n.doubleValue()) , evaluatedArguments));

                // 📊 สถิติ
                case "sum":
                    return new Value<>(new QueryOperationNode(new SumOperation(), evaluatedArguments));

                case "mean":
                    return new Value<>(new QueryOperationNode(new MeanOperation(), evaluatedArguments));

                case "median":
                    return new Value<>(new QueryOperationNode(new MedianOperation(), evaluatedArguments));

                case "mode":
                    return new Value<>(new QueryOperationNode(new ModeOperation(), evaluatedArguments));

                case "count":
                    return new Value<>(new QueryOperationNode(new CountOperation(), evaluatedArguments));

                case "range":
                    return new Value<>(new QueryOperationNode(new RangeOperation(), evaluatedArguments));

                case "product":
                    return new Value<>(new QueryOperationNode(new ProductOperation(), evaluatedArguments));


                // 💡 ตรรกะ/เงื่อนไข
                case "equals":
                    if (evaluatedArguments.get(1).get() == null){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new EqualsOperation(evaluatedArguments.get(1)) , evaluatedArguments));
                case "not":
                    return new Value<>(new QueryOperationNode(new NotOperation() , evaluatedArguments));

                // 📝 สตริง
                case "length":
                    return new Value<>(new QueryOperationNode(new LengthOperation() , evaluatedArguments));
                case "upper":
                    return new Value<>(new QueryOperationNode(new UpperOperation() , evaluatedArguments));
                case "lower":
                    return new Value<>(new QueryOperationNode(new LowerOperation() , evaluatedArguments));
                case "trim":
                    return new Value<>(new QueryOperationNode(new TrimOperation() , evaluatedArguments));
                case "concat":
                    if (!(evaluatedArguments.get(1).get() instanceof String s)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new ConcatOperation(s) , evaluatedArguments));
                case "substring":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    if (!(evaluatedArguments.get(1).get() instanceof Number n2)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new SubstringOperation(n1.intValue() , n2.intValue()) , evaluatedArguments));
                case "replace":
                    if (!(evaluatedArguments.get(1).get() instanceof String s1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    if (!(evaluatedArguments.get(1).get() instanceof String s2)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new ReplaceOperation(s1 , s2) , evaluatedArguments));
                case "indexof":
                    if (!(evaluatedArguments.get(1).get() instanceof String s1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new IndexOfOperation(s1) , evaluatedArguments));
                case "startswith":
                    if (!(evaluatedArguments.get(1).get() instanceof String s1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new StartsWithOperation(s1) , evaluatedArguments));
                case "endswith":
                    if (!(evaluatedArguments.get(1).get() instanceof String s1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new EndsWithOperation(s1) , evaluatedArguments));
                case "split":
                    return new Value<>(new QueryOperationNode(new SplitOperation() , evaluatedArguments));
                case "join":
                    if (!(evaluatedArguments.get(1).get() instanceof String s1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new JoinOperation(s1) , evaluatedArguments));
                case "reverse":
                    return new Value<>(new QueryOperationNode(new ReverseOperation(), evaluatedArguments));
                case "contains":
                    if (!(evaluatedArguments.get(1).get() instanceof String s1)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be string");
                    }
                    return new Value<>(new QueryOperationNode(new ContainsOperation(s1), evaluatedArguments));
                case "padleft":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    if (!(evaluatedArguments.get(1).get() instanceof Character c)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be character");
                    }
                    return new Value<>(new QueryOperationNode(new PadLeftOperation(n.intValue() , c), evaluatedArguments));
                case "padright":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    if (!(evaluatedArguments.get(1).get() instanceof Character c)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be character");
                    }
                    return new Value<>(new QueryOperationNode(new PadRightOperation(n.intValue() , c), evaluatedArguments));
                case "repeat":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new RepeatOperation(n.intValue()), evaluatedArguments));
                case "tostring":
                    return new Value<>(new QueryOperationNode(new ToStringOperation(), evaluatedArguments));

                // 📚 การจัดการรายการ/คอลเลกชัน
                case "sort":
                    return  new Value<>(new QueryOperationNode(new SortOperation(), evaluatedArguments));
                case "distinct":
                    return new Value<>(new QueryOperationNode(new DistinctOperation(), evaluatedArguments));


                // ⚙️ การจัดการข้อมูล/ประเภท
                case "type":
                    return new Value<>(new QueryOperationNode(new TypeOperation(), evaluatedArguments));
                case "isnumber":
                    return new Value<>(new QueryOperationNode(new IsNumberOperation(), evaluatedArguments));
                case "isstring":
                    return new Value<>(new QueryOperationNode(new IsStringOperation(), evaluatedArguments));
                case "islist":
                    return new Value<>(new QueryOperationNode(new IsListOperation(), evaluatedArguments));
                case "isbool":
                    return new Value<>(new QueryOperationNode(new IsBoolOperation(), evaluatedArguments));
                case "print":
                    return new Value<>(new QueryOperationNode(new PrintOperation(client), evaluatedArguments));
                case "coalesce":
                    if (evaluatedArguments.get(1).get() == null){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new CoalesceOperation(evaluatedArguments.get(1)), evaluatedArguments));

                // 🗂️ การจัดการรายการแบบมีเงื่อนไข
                case "take":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new TakeOperation(n.intValue()), evaluatedArguments));

                case "skip":
                    if (!(evaluatedArguments.get(1).get() instanceof Number n)) {
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new SkipOperation(n.intValue()), evaluatedArguments
                    ));


                case "fill":
                    if (evaluatedArguments.get(1).get() == null){
                        throw new KException(ExceptionCode.KDE0012, "exponent should be number");
                    }
                    return new Value<>(new QueryOperationNode(new FillOperation(evaluatedArguments.get(1) , evaluatedArguments.get(2)), evaluatedArguments));
                case "connect":
                    ConnectionFunction connectionFunction = new ConnectionFunction();
                    connectionFunction.addArgument( "databaseType" , evaluatedArguments.get(0));
                    connectionFunction.addArgument("databaseName" , evaluatedArguments.get(1));
                    connectionFunction.addArgument("host" , evaluatedArguments.get(2));
                    connectionFunction.addArgument("port" , evaluatedArguments.get(3));
                    connectionFunction.addArgument("dataCentre" , evaluatedArguments.get(4));
                    connectionFunction.addArgument("user" , evaluatedArguments.get(4));
                    connectionFunction.addArgument("pass" , evaluatedArguments.get(5));
                    connectionFunction.execute();
                case "download":
                    DownloadFunction downloadFunction = new DownloadFunction();
                case "export":
                    ExportFunction exportFunction = new ExportFunction();
                    exportFunction.addArgument( "dataframe" , evaluatedArguments.get(0));
                    exportFunction.addArgument("dataType" , evaluatedArguments.get(1));
                    exportFunction.addArgument("fileName" , evaluatedArguments.get(2));
                    exportFunction.addArgument("userId" , evaluatedArguments.get(3));
                    exportFunction.execute();
                case "fetch":
                    FetchFunction fetchFunction = new FetchFunction();
                    fetchFunction.addArgument( "datatype" , evaluatedArguments.get(0));
                    fetchFunction.addArgument( "datasource" , evaluatedArguments.get(1));
                    fetchFunction.addArgument( "memoryGroupName" , evaluatedArguments.get(2));
                    fetchFunction.addArgument( "query" , evaluatedArguments.get(3));
                    fetchFunction.execute();
                case "remove":
                    RemoveFunction removeFunction = new RemoveFunction();
                    removeFunction.addArgument( "dataName" , evaluatedArguments.get(0));
                    removeFunction.addArgument( "session" , new Value<>(client.getCurrentSession()));
                    removeFunction.execute();
                case "apply":
                    ApplyFunction applyFunction = new ApplyFunction();
                    applyFunction.addArgument("session", new Value<>(client.getCurrentSession()));
                    if(evaluatedArguments.get(0).get() instanceof QueryOperationNode node){
                        QueryExecution execution = new QueryExecution();
                        execution.getHead().next(node);
                        applyFunction.addArgument("dataframe", new Value<>(execution));
                    }else {
                        applyFunction.addArgument("dataframe", evaluatedArguments.getFirst());
                    }
                    applyFunction.addArgument("operation", evaluatedArguments.get(1));
                    applyFunction.execute();
                case "user":
                    UserCommand userCommand = new UserCommand();
                    userCommand.addArgument( "command" , evaluatedArguments.get(0));

                    userCommand.addArgument( "name" , evaluatedArguments.get(1));
                    userCommand.addArgument( "maxSession" , evaluatedArguments.get(2));
                    userCommand.addArgument( "maxProcessPerSession" , evaluatedArguments.get(3));
                    userCommand.addArgument( "maxMemoryPerProcess" , evaluatedArguments.get(4));
                    userCommand.addArgument( "maxStorageUsage" , evaluatedArguments.get(5));
                    userCommand.addArgument( "password" , evaluatedArguments.get(6));
                    userCommand.addArgument( "isAdmin" , evaluatedArguments.get(7));

                    userCommand.addArgument( "userId" , evaluatedArguments.get(1));
                    userCommand.addArgument( "name" , evaluatedArguments.get(2));
                    userCommand.addArgument( "maxSession" , evaluatedArguments.get(3));
                    userCommand.addArgument( "maxProcessPerSession" , evaluatedArguments.get(4));
                    userCommand.addArgument( "maxMemoryPerProcess" , evaluatedArguments.get(5));
                    userCommand.addArgument( "maxStorageUsage" , evaluatedArguments.get(6));

                    userCommand.addArgument( "userId" , evaluatedArguments.get(1));
                    userCommand.execute();
                case "task":
                    TaskCommand taskCommand = new TaskCommand();
                    taskCommand.addArgument( "command" , evaluatedArguments.get(0));

                    taskCommand.addArgument( "taskId" , evaluatedArguments.get(1));
                    taskCommand.addArgument( "userId" , evaluatedArguments.get(2));

                    taskCommand.addArgument( "sessionId" , evaluatedArguments.get(1));
                    taskCommand.execute();
                case "session":
                    SessionCommand sessionCommand = new SessionCommand();
                    sessionCommand.addArgument( "command" , evaluatedArguments.get(0));

                    sessionCommand.addArgument( "sessionId" , evaluatedArguments.get(1));
                    sessionCommand.execute();
                default:
                    throw new KException(ExceptionCode.KD00006, "There is no this command");

            }
        } else if(expression instanceof Pipeline pipe){ //return query execution
            // เริ่มต้นด้วยค่า null สำหรับ pipeline แรก

            QueryExecution queryExecution = new QueryExecution();
            QueryOperationNode queryOperationNode = queryExecution.getHead();

            for (Expression expr : pipe.pipeline.toArray()) {
                // ถ้าเป็น FunctionCall ให้ประมวลผลพิเศษ
                if (expr instanceof FunctionCall fc) {
                    Value<?> value = evaluateExpression(expr, client);

                    if (!(value.get() instanceof QueryOperationNode queryOperationNode2)){
                        throw new KException(ExceptionCode.KD00006, "only function allow in pipeline");
                    }

                    queryOperationNode.next(queryOperationNode2);
                    queryOperationNode = queryOperationNode2;
                } else {
                    throw new KException(ExceptionCode.KD00006, "only function allow in pipeline");
                }
            }

            return new Value<>(queryExecution);

        }else if(expression instanceof StringLiteral str){
            return new Value<>(new String(str.literal));
        } else if(expression instanceof Subscript sub){
            // ประเมินฐาน (base) และดัชนี (index)
            Value<?> baseVal = evaluateExpression(sub.base, client);
            Value<?> indexVal = evaluateExpression(sub.index, client);

            // ตรวจสอบชนิดของฐาน
            Object base = baseVal.get();

            if (base instanceof List list) {
                // การเข้าถึงองค์ประกอบของ List
                if (indexVal.get() instanceof Number indexNum) {
                    int index = indexNum.intValue();
                    if (index >= 0 && index < list.size()) {
                        return new Value<>(list.get(index));
                    } else {
                        throw new KException(ExceptionCode.KDC0003, "List index out of bounds: " + index);
                    }
                } else {
                    throw new KException(ExceptionCode.KDC0003, "List index must be a number");
                }
            } else if (base instanceof String str) {
                // การเข้าถึงตัวอักษรของ String
                if (indexVal.get() instanceof Number indexNum) {
                    int index = indexNum.intValue();
                    if (index >= 0 && index < str.length()) {
                        return new Value<>(String.valueOf(str.charAt(index)));
                    } else {
                        throw new KException(ExceptionCode.KDC0003, "String index out of bounds: " + index);
                    }
                } else {
                    throw new KException(ExceptionCode.KDC0003, "String index must be a number");
                }
            }else if(base instanceof ColumnArray columnArray){
                if(indexVal.get() instanceof String s){
                    return new Value<>(new ColumnArray(new ImmutableArray<>(new Column[]{columnArray.getColumns().get(s)}), client.getCurrentSession().getSessionData().getMemoryGroup()));
                }else if(indexVal.get() instanceof List list){
                    if(list.isEmpty()){
                        return new Value<>(new ColumnArray(new ImmutableArray<>(new Column[0]), client.getCurrentSession().getSessionData().getMemoryGroup()));
                    }else if(list.getFirst() instanceof String){
                        List<Column> col = columnArray.getColumns().keySet().stream().map(x -> columnArray.getColumns().get(x)).toList();
                        return new Value<>(new ColumnArray(new ImmutableArray<>(col), client.getCurrentSession().getSessionData().getMemoryGroup()));
                    }
                }else {
                    throw new KException(ExceptionCode.KD00006, "Cannot subscript");
                }
            }

            throw new KException(ExceptionCode.KDC0003, "Subscript operation not supported for type: " + base.getClass().getSimpleName());
        }else if(expression instanceof UnaryExpression unary){
            // ประเมินนิพจน์ที่อยู่ด้านขวาของ Unary Operator
            Value<?> exprVal = evaluateExpression(unary.expression, client);
            Object value = exprVal.get();

            switch (unary.op) {
                case SUB -> { // Negative (เช่น -5)
                    if (value instanceof Number n) {
                        return new Value<>(-n.doubleValue());
                    }
                }
                default -> {
                    throw new KException(ExceptionCode.KDC0001, "unknown operator");
                }
            }
            // หากไม่สามารถประมวลผลได้ (เช่น ชนิดข้อมูลไม่ถูกต้องสำหรับ operator นั้น)
            throw new KException(ExceptionCode.KDC0003, "Unary operation '" + unary.op + "' not supported for type: " + value.getClass().getSimpleName());
        }
        return new NullValue("Invalid");

    }
}