package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.database.DatabaseConnection;
import hello1.koddata.dataframe.Column; // สมมติว่ามี class นี้
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.dataframe.loader.CSVLoader;
import hello1.koddata.dataframe.loader.DataFrameLoader;
import hello1.koddata.dataframe.loader.DatabaseLoader;
import hello1.koddata.engine.DataSource;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import hello1.koddata.sessions.Session;
import hello1.koddata.utils.collection.ImmutableArray;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

// เปลี่ยน Generic Type เป็น Column[]
public class FetchFunction extends KodFunction<ColumnArray> {

    @Override
    public Value<ColumnArray> execute() throws KException {
        // --- Validation Section ---
        if (!arguments.containsKey("datatype")) {
            System.out.println("dtype");
            throw new KException(ExceptionCode.KDE0012, "Function fetch need argument datatype");
        }
        if (!arguments.containsKey("datasource")) {
            System.out.println("ds");
            throw new KException(ExceptionCode.KDE0012, "Function fetch need argument datasource");
        }

        Value<?> value = arguments.get("datatype");
        Value<?> source = arguments.get("datasource");

        Value<DataSource> dataSourceValue = null;

        if (value.get() instanceof DataSource dataSource) {
            dataSourceValue = new Value<>(dataSource);
        } else {
            System.out.println("csv");
            throw new KException(ExceptionCode.KDE0012, "argument Data Source should be DATABASE , CSV");
        }

        if (!arguments.containsKey("session")) {
            System.out.println("ds");
            throw new KException(ExceptionCode.KDE0012, "Function fetch need argument session");
        }

        Value<?> session = arguments.get("session");

        if(!(session.get() instanceof Session session1)){
            throw new KException(ExceptionCode.KDE0012, "error");
        }


        final Value<DataSource> finalDataSource = dataSourceValue;
        DataFrameLoader dataFrameLoader = null;

        // --- Logic Execution (Synchronous) ---
        if (dataSourceValue.get().equals(DataSource.CSV)) {
            if (source.get() instanceof String s) {
                final File file = new File(Main.bootstrap.getRootPath().resolve("home").resolve(session1.getUser().getUser().getUserData().name()).resolve(s).toAbsolutePath().toString());
                System.out.println(Main.bootstrap.getRootPath().resolve("home").resolve(session1.getUser().getUser().getUserData().name()).resolve(s).toAbsolutePath().toString());
                if (!file.exists()) {
                    throw new KException(ExceptionCode.KDE0013, "There are no file in that name exists");
                }

                // สร้าง Loader
                dataFrameLoader = new CSVLoader();

                // โหลดข้อมูลทันที (รอจนเสร็จ)
                try {
                    dataFrameLoader.load(new FileInputStream(file));
                } catch (IOException e) {
                    throw new KException(ExceptionCode.KDE0013, "Error loading CSV file: " + e.getMessage());
                }
            } else {
                System.out.println("string");
                throw new KException(ExceptionCode.KDE0012, "source can only use string");
            }

        } else {
            // Database Logic
            if (source.get() instanceof DatabaseConnection databaseConnection) {
                if (!arguments.containsKey("query")) {
                    throw new KException(ExceptionCode.KDE0012, "Function fetch database need argument query");
                }

                Value<?> query = arguments.get("query");
                if (query.get() instanceof String queryString) {

                    // สร้าง Loader
                    dataFrameLoader = new DatabaseLoader(databaseConnection, queryString);

                    // โหลดข้อมูลทันที (รอจนเสร็จ)
                    try {
                        dataFrameLoader.load(null);
                    } catch (IOException e) {
                        throw new KException(ExceptionCode.KDE0013, "Error loading from Database: " + e.getMessage());
                    }
                } else {
                    throw new KException(ExceptionCode.KDE0012, "query should be string");
                }

            } else {
                throw new KException(ExceptionCode.KDE0012, "database can only use databaseConnection");
            }
        }

        // --- Return Result ---
        // สมมติว่า dataFrameLoader มีเมธอด getColumns() เพื่อดึง Column[] ออกมา
        // หาก method ชื่ออื่นให้เปลี่ยนตรงนี้ครับ เช่น dataFrameLoader.getResult()
        if (dataFrameLoader != null) {
            return new Value<>(new ColumnArray(new ImmutableArray<>(dataFrameLoader.getColumns())));
        } else {
            throw new KException(ExceptionCode.KDE0013, "Loader failed to initialize");
        }
    }
}