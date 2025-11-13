package hello1.koddata;

import hello1.koddata.engine.Bootstrap;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.Lexer;
import hello1.koddata.kodlang.Parser;
import hello1.koddata.kodlang.Token;
import hello1.koddata.kodlang.ast.*;
import hello1.koddata.utils.collection.ImmutableArray;
import hello1.koddata.utils.collection.primitive.lists.OffHeapShortList;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    public static Bootstrap bootstrap;

    public static void main(String[] args){
        bootstrap = new Bootstrap();
        bootstrap.start(args);
//        String test = "$a <- max 12**(3-5) \"hello\";";
//        Token[] token = Lexer.analyze(test.toCharArray());
//        Parser parser = new Parser(new ImmutableArray<>(token));
//        Statement statement = parser.parseStatement();
//        String testaa = ASTToString.astToString(statement);
//        System.out.println(testaa);
//        SemanticAnalyzer.analyze(statement);
    }
}
