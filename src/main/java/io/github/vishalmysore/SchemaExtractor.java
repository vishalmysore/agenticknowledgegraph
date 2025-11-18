package io.github.vishalmysore;

import com.kuzudb.*;
import java.io.File;

public class SchemaExtractor {

       // private static final String db_path = "c:/work/agenticgraph/db/yoga.kuzu";

        public static String getSchemaForDB(String dbPath, String type) {
            StringBuffer schemaInfo = new StringBuffer();
            try (Database db = new Database(dbPath);
                 Connection conn = new Connection(db)) {

                schemaInfo.append("--- Schema Information For ").append(type).append(" Knowledge Graph ---\n");

                QueryResult tables = conn.query("CALL SHOW_TABLES() RETURN *");

                while (tables.hasNext()) {
                    FlatTuple tuple = tables.getNext();
                    String tableName = (String) tuple.getValue(1).getValue();
                    String tableType = (String) tuple.getValue(2).getValue();

                    schemaInfo.append(tableName).append(" (").append(tableType).append("):");

                    String query = String.format("CALL table_info('%s') RETURN *", tableName);
                    QueryResult columns = conn.query(query);

                    while (columns.hasNext()) {
                        FlatTuple col = columns.getNext();
                        System.out.println(col.toString());

                    }
                    schemaInfo.append("End of ").append(tableName).append("\n\n");

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return schemaInfo.toString();
        }
    }