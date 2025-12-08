package hello1.koddata;

import hello1.koddata.engine.Bootstrap;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.Lexer;
import hello1.koddata.kodlang.Parser;
import hello1.koddata.kodlang.Token;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.utils.collection.ImmutableArray;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    public static Bootstrap bootstrap;

    public static void main(String[] args) throws IOException, KException, InterruptedException {
        bootstrap = new Bootstrap();
        bootstrap.start(args);
//        String testCode = """
//                $pipe <- pipeline{
//                    sort date;
//                    fill null, 0;
//                };
//                """;
//        Token[] t = Lexer.analyze(testCode.toCharArray());
//        Parser parser = new Parser(new ImmutableArray<>(t));
//        Statement statement = parser.parseStatement();
//        System.out.println(ASTToString.astToString(statement));
    }
}
