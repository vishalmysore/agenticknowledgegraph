package io.github.vishalmysore;

import com.kuzudb.Database;
import com.kuzudb.FlatTuple;
import com.kuzudb.QueryResult;
import com.kuzudb.*;

public class FraudDetection {

    public static void main(String[] args) {
        // Create an empty on-disk database and connect to it
        Database db = new Database("fraud_detection.kuzu");
        Connection conn = new Connection(db);

        // Create tables
        conn.query("CREATE NODE TABLE FraudType(name STRING PRIMARY KEY, description STRING)");
        conn.query("CREATE NODE TABLE DetectionMethod(name STRING PRIMARY KEY, description STRING)");
        conn.query("CREATE NODE TABLE Indicator(name STRING PRIMARY KEY, description STRING)");
        conn.query("CREATE NODE TABLE DataSource(name STRING PRIMARY KEY, description STRING)");
        conn.query("CREATE REL TABLE Detects(FROM DetectionMethod TO FraudType, confidence INT64)");
        conn.query("CREATE REL TABLE Uses(FROM DetectionMethod TO Indicator)");
        conn.query("CREATE REL TABLE Analyzes(FROM DetectionMethod TO DataSource)");

        // Load data
        conn.query("COPY FraudType FROM 'src/main/resources/fraud/fraud-type.csv'");
        conn.query("COPY DetectionMethod FROM 'src/main/resources/fraud/detection-method.csv'");
        conn.query("COPY Indicator FROM 'src/main/resources/fraud/indicator.csv'");
        conn.query("COPY DataSource FROM 'src/main/resources/fraud/data-source.csv'");
        conn.query("COPY Detects FROM 'src/main/resources/fraud/detects.csv'");
        conn.query("COPY Uses FROM 'src/main/resources/fraud/uses.csv'");
        conn.query("COPY Analyzes FROM 'src/main/resources/fraud/analyzes.csv'");

        // Query 1: Simple - All detection methods and fraud types they detect
        System.out.println("\n=== Query 1: Detection Methods and Fraud Types They Detect ===");
        QueryResult result = conn.query(
                "MATCH (dm:DetectionMethod)-[d:Detects]->(ft:FraudType) " +
                "RETURN dm.name, d.confidence, ft.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 2: Detection methods with their indicators
        System.out.println("\n=== Query 2: Detection Methods and Their Indicators ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[u:Uses]->(i:Indicator) " +
                "RETURN dm.name, i.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 3: Complex - Detection methods with fraud types and indicators
        System.out.println("\n=== Query 3: Detection Methods with Fraud Types and Indicators ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[d:Detects]->(ft:FraudType), " +
                "(dm)-[u:Uses]->(i:Indicator) " +
                "RETURN dm.name, ft.name, i.name, d.confidence;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 4: Detection methods with data sources they analyze
        System.out.println("\n=== Query 4: Detection Methods and Data Sources They Analyze ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[a:Analyzes]->(ds:DataSource) " +
                "RETURN dm.name, ds.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 5: High confidence fraud detection (confidence > 80)
        System.out.println("\n=== Query 5: High Confidence Fraud Detection (>80%) ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[d:Detects]->(ft:FraudType) " +
                "WHERE d.confidence > 80 " +
                "RETURN dm.name, d.confidence, ft.name " +
                "ORDER BY d.confidence DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 6: Fraud types and detection methods that can detect them
        System.out.println("\n=== Query 6: Fraud Types with Detection Methods ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[d:Detects]->(ft:FraudType) " +
                "WITH ft, COUNT(dm) as method_count, COLLECT(dm.name) as methods, AVG(d.confidence) as avg_confidence " +
                "RETURN ft.name, method_count, avg_confidence;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 7: Detection methods with most indicators
        System.out.println("\n=== Query 7: Detection Methods Ranked by Number of Indicators ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[u:Uses]->(i:Indicator) " +
                "WITH dm, COUNT(i) as indicator_count " +
                "RETURN dm.name, indicator_count " +
                "ORDER BY indicator_count DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 8: Complete detection workflow - method -> fraud type -> indicator -> data source
        System.out.println("\n=== Query 8: Complete Fraud Detection Workflow ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[d:Detects]->(ft:FraudType), " +
                "(dm)-[u:Uses]->(i:Indicator), " +
                "(dm)-[a:Analyzes]->(ds:DataSource) " +
                "RETURN dm.name, ft.name, i.name, ds.name, d.confidence;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 9: Indicators used across multiple detection methods
        System.out.println("\n=== Query 9: Indicators Used by Multiple Detection Methods ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[u:Uses]->(i:Indicator) " +
                "WITH i, COUNT(dm) as method_count " +
                "WHERE method_count > 1 " +
                "RETURN i.name, method_count " +
                "ORDER BY method_count DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 10: Data sources analyzed by multiple detection methods
        System.out.println("\n=== Query 10: Data Sources Analyzed by Multiple Methods ===");
        result = conn.query(
                "MATCH (dm:DetectionMethod)-[a:Analyzes]->(ds:DataSource) " +
                "WITH ds, COUNT(dm) as method_count, COLLECT(dm.name) as methods " +
                "RETURN ds.name, method_count " +
                "ORDER BY method_count DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }
    }
}
