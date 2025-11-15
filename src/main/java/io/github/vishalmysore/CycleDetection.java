package io.github.vishalmysore;

import com.kuzudb.Database;
import com.kuzudb.FlatTuple;
import com.kuzudb.QueryResult;
import com.kuzudb.*;
import java.util.*;

public class CycleDetection {

    private static final double CYCLE_RISK_THRESHOLD = 0.7;

    public static void main(String[] args) {
        // Create an empty on-disk database and connect to it
        Database db = new Database("cycle_detection.kuzu");
        Connection conn = new Connection(db);

        // Create tables
        conn.query("CREATE NODE TABLE Account(account_id STRING PRIMARY KEY, account_type STRING, risk_score DOUBLE)");
        conn.query("CREATE NODE TABLE Transaction(transaction_id STRING PRIMARY KEY, amount DOUBLE, timestamp STRING)");
        conn.query("CREATE NODE TABLE CyclePattern(pattern_id STRING PRIMARY KEY, pattern_name STRING, description STRING, risk_level STRING)");
        conn.query("CREATE NODE TABLE Algorithm(algorithm_name STRING PRIMARY KEY, description STRING, time_complexity STRING)");

        conn.query("CREATE REL TABLE Transfers(FROM Account TO Account, transaction_id STRING, amount DOUBLE)");
        conn.query("CREATE REL TABLE Involves(FROM Transaction TO Account)");
        conn.query("CREATE REL TABLE DetectsPattern(FROM Algorithm TO CyclePattern, confidence INT64)");

        // Load data
        conn.query("COPY Account FROM 'src/main/resources/cycle/account.csv'");
        conn.query("COPY Transaction FROM 'src/main/resources/cycle/transaction.csv'");
        conn.query("COPY CyclePattern FROM 'src/main/resources/cycle/cycle-pattern.csv'");
        conn.query("COPY Algorithm FROM 'src/main/resources/cycle/algorithm.csv'");
        conn.query("COPY Transfers FROM 'src/main/resources/cycle/transfers.csv'");
        conn.query("COPY Involves FROM 'src/main/resources/cycle/involves.csv'");
        conn.query("COPY DetectsPattern FROM 'src/main/resources/cycle/detects-pattern.csv'");

        // Query 1: Simple - All account transfers
        System.out.println("\n=== Query 1: All Account Transfers ===");
        QueryResult result = conn.query(
                "MATCH (a1:Account)-[t:Transfers]->(a2:Account) " +
                "RETURN a1.account_id, t.amount, a2.account_id;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 2: High-risk accounts
        System.out.println("\n=== Query 2: High-Risk Accounts ===");
        result = conn.query(
                "MATCH (a:Account) WHERE a.risk_score > 0.7 " +
                "RETURN a.account_id, a.account_type, a.risk_score " +
                "ORDER BY a.risk_score DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 3: Detect potential 2-step cycles (A->B->A)
        System.out.println("\n=== Query 3: Potential 2-Step Cycles (A->B->A) ===");
        result = conn.query(
                "MATCH (a1:Account)-[t1:Transfers]->(a2:Account)-[t2:Transfers]->(a3:Account) " +
                "WHERE a1.account_id = a3.account_id " +
                "RETURN a1.account_id, a2.account_id, t1.amount, t2.amount;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 4: Detect potential 3-step cycles (A->B->C->A)
        System.out.println("\n=== Query 4: Potential 3-Step Cycles (A->B->C->A) ===");
        result = conn.query(
                "MATCH (a1:Account)-[t1:Transfers]->(a2:Account)-[t2:Transfers]->(a3:Account)-[t3:Transfers]->(a4:Account) " +
                "WHERE a1.account_id = a4.account_id " +
                "RETURN a1.account_id, a2.account_id, a3.account_id, t1.amount, t2.amount, t3.amount;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 5: Detect potential 4-step cycles (A->B->C->D->A) - MAIN CYCLE
        System.out.println("\n=== Query 5: Potential 4-Step Cycles (A->B->C->D->A) ===");
        result = conn.query(
                "MATCH (a1:Account)-[t1:Transfers]->(a2:Account)-[t2:Transfers]->(a3:Account)-[t3:Transfers]->(a4:Account)-[t4:Transfers]->(a5:Account) " +
                "WHERE a1.account_id = a5.account_id " +
                "RETURN a1.account_id, a2.account_id, a3.account_id, a4.account_id, t1.amount, t2.amount, t3.amount, t4.amount;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 6: Account outgoing vs incoming transfers
        System.out.println("\n=== Query 6: Account Transfer Summary (Outgoing and Incoming) ===");
        result = conn.query(
                "MATCH (a:Account)-[out:Transfers]->(out_account:Account) " +
                "WITH a, COUNT(out) as outgoing_transfers, SUM(out.amount) as total_outgoing " +
                "MATCH (in_account:Account)-[in_trans:Transfers]->(a) " +
                "WITH a, outgoing_transfers, total_outgoing, COUNT(in_trans) as incoming_transfers, SUM(in_trans.amount) as total_incoming " +
                "RETURN a.account_id, outgoing_transfers, total_outgoing, incoming_transfers, total_incoming " +
                "ORDER BY total_outgoing DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 7: Cycle patterns and detection algorithms
        System.out.println("\n=== Query 7: Cycle Patterns and Detection Algorithms ===");
        result = conn.query(
                "MATCH (algo:Algorithm)-[d:DetectsPattern]->(pattern:CyclePattern) " +
                "RETURN algo.algorithm_name, pattern.pattern_name, d.confidence, pattern.risk_level;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 8: Complex - Accounts in cycles with their pattern details
        System.out.println("\n=== Query 8: High-Risk Cycle Analysis ===");
        result = conn.query(
                "MATCH (a:Account) WHERE a.risk_score > 0.7 " +
                "MATCH (a)-[t:Transfers]->(next:Account) " +
                "WITH a, next, t " +
                "MATCH (next)-[t2:Transfers]->(target:Account) " +
                "RETURN a.account_id, next.account_id, target.account_id, a.risk_score, t.amount, t2.amount " +
                "ORDER BY a.risk_score DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 9: Transactions involved in accounts
        System.out.println("\n=== Query 9: Transactions Linked to Accounts ===");
        result = conn.query(
                "MATCH (t:Transaction)-[i:Involves]->(a:Account) " +
                "RETURN t.transaction_id, t.amount, t.timestamp, a.account_id;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 10: Total cycle risk assessment
        System.out.println("\n=== Query 10: Cycle Risk Assessment Summary ===");
        result = conn.query(
                "MATCH (pattern:CyclePattern) " +
                "RETURN pattern.pattern_name, pattern.risk_level, pattern.description;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // CYCLE DETECTION LOGIC
        System.out.println("\n\n========== CYCLE DETECTION ANALYSIS ==========");
        performCycleDetectionAnalysis(conn);
    }

    /**
     * Performs cycle detection analysis using DFS algorithm
     */
    private static void performCycleDetectionAnalysis(Connection conn) {
        System.out.println("\n--- Executing DFS-based Cycle Detection ---\n");

        // Query to get all accounts and their transfers
        QueryResult accountsResult = conn.query(
                "MATCH (a:Account)-[t:Transfers]->(b:Account) " +
                "RETURN a.account_id, b.account_id, t.amount;");

        Map<String, List<String>> graph = new HashMap<>();
        Map<String, Double> accountRisks = new HashMap<>();
        Map<String, Double> transferAmounts = new HashMap<>();

        // Build adjacency list
        while (accountsResult.hasNext()) {
            FlatTuple row = accountsResult.getNext();
            String from = row.getValue(0).toString();
            String to = row.getValue(1).toString();

            graph.computeIfAbsent(from, k -> new ArrayList<>()).add(to);

            // Store edge weight (amount)
            transferAmounts.put(from + "->" + to, Double.parseDouble(row.getValue(2).toString()));
        }

        // Get all account risk scores
        QueryResult riskResult = conn.query(
                "MATCH (a:Account) RETURN a.account_id, a.risk_score;");
        while (riskResult.hasNext()) {
            FlatTuple row = riskResult.getNext();
            accountRisks.put(row.getValue(0).toString(), Double.parseDouble(row.getValue(1).toString()));
        }

        // Perform DFS cycle detection
        Set<String> visited = new HashSet<>();
        Set<String> recStack = new HashSet<>();
        List<List<String>> cycles = new ArrayList<>();

        for (String account : graph.keySet()) {
            if (!visited.contains(account)) {
                List<String> currentPath = new ArrayList<>();
                dfsDetectCycle(account, graph, visited, recStack, currentPath, cycles);
            }
        }

        // Print detected cycles
        if (cycles.isEmpty()) {
            System.out.println("✓ No cycles detected in the transaction network");
        } else {
            System.out.println("⚠️  " + cycles.size() + " CYCLE(S) DETECTED:\n");
            for (int i = 0; i < cycles.size(); i++) {
                List<String> cycle = cycles.get(i);
                System.out.println("Cycle " + (i + 1) + ":");
                System.out.print("  Path: ");
                for (int j = 0; j < cycle.size(); j++) {
                    System.out.print(cycle.get(j));
                    if (j < cycle.size() - 1) System.out.print(" → ");
                }
                System.out.println();

                // Calculate cycle metrics
                double totalAmount = 0;
                double avgRisk = 0;
                int count = 0;

                for (String account : cycle) {
                    if (accountRisks.containsKey(account)) {
                        avgRisk += accountRisks.get(account);
                        count++;
                    }
                }

                if (count > 0) {
                    avgRisk = avgRisk / count;
                }

                System.out.println("  Cycle Length: " + cycle.size() + " accounts");
                System.out.println("  Average Risk Score: " + String.format("%.2f", avgRisk));
                System.out.println("  Status: " + (avgRisk > CYCLE_RISK_THRESHOLD ? "🔴 HIGH RISK - INVESTIGATE" : "🟡 MONITOR"));
                System.out.println();
            }
        }

        // Print network statistics
        System.out.println("\n--- Network Statistics ---");
        System.out.println("Total Accounts: " + accountRisks.size());
        System.out.println("Total Transfer Relationships: " + transferAmounts.size());

        long highRiskCount = accountRisks.values().stream()
                .filter(risk -> risk > CYCLE_RISK_THRESHOLD)
                .count();
        System.out.println("High-Risk Accounts: " + highRiskCount);
    }

    /**
     * DFS algorithm to detect cycles in the transaction graph
     */
    private static void dfsDetectCycle(String account, Map<String, List<String>> graph,
                                      Set<String> visited, Set<String> recStack,
                                      List<String> path, List<List<String>> cycles) {
        visited.add(account);
        recStack.add(account);
        path.add(account);

        if (graph.containsKey(account)) {
            for (String neighbor : graph.get(account)) {
                if (!visited.contains(neighbor)) {
                    dfsDetectCycle(neighbor, graph, visited, recStack, path, cycles);
                } else if (recStack.contains(neighbor)) {
                    // Found a cycle
                    int cycleStart = path.indexOf(neighbor);
                    List<String> cycle = new ArrayList<>(path.subList(cycleStart, path.size()));
                    cycle.add(neighbor); // Complete the cycle
                    cycles.add(cycle);
                }
            }
        }

        path.remove(path.size() - 1);
        recStack.remove(account);
    }
}

