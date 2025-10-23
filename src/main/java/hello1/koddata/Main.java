package hello1.koddata;

import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.sessions.VariablePool;
import hello1.koddata.utils.collection.ImmutableArray;
import hello1.koddata.utils.collection.primitive.lists.OffHeapShortList;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException, KException {
        Statement stmt = new BlockStatement(new ImmutableArray<>(List.of(
                new AssignmentExpression(new Identifier("test"), new BinaryExpression(BinaryExpression.Operator.ADD, new NumberLiteral("12".toCharArray(), false), new NumberLiteral("12".toCharArray(), false))),
                new SelectStatement(new ImmutableArray<>(List.of(
                        new ProjectionExpression(new Identifier("test"), new Identifier("t"))
                )), new DataFrameDeclaration(new Identifier("df"), new Identifier("d")), null, new BinaryExpression(BinaryExpression.Operator.EQUALS, new Identifier("age"), new NumberLiteral("12".toCharArray(), false)), null, null)
        )));
        String rep = ASTToString.astToString(stmt);
        System.out.println(rep);
        SemanticAnalyzer.analyze(new NumberLiteral("1378921".toCharArray(), false));
    }
}
