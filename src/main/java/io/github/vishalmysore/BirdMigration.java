package io.github.vishalmysore;

import com.kuzudb.Database;
import com.kuzudb.FlatTuple;
import com.kuzudb.QueryResult;
import com.kuzudb.*;
import java.util.*;

public class BirdMigration {

    private static final double MIGRATION_CYCLE_THRESHOLD = 8000;

    public static void main(String[] args) {
        // Create an empty on-disk database and connect to it
        Database db = new Database("bird_migration.kuzu");
        Connection conn = new Connection(db);

        // Create tables
        conn.query("CREATE NODE TABLE BirdSpecies(species_name STRING PRIMARY KEY, migration_distance DOUBLE, flight_duration INT64)");
        conn.query("CREATE NODE TABLE Location(location_name STRING PRIMARY KEY, location_type STRING, habitat_quality DOUBLE)");
        conn.query("CREATE NODE TABLE Season(season_name STRING PRIMARY KEY, month_range STRING, temperature_range STRING)");
        conn.query("CREATE NODE TABLE EnvironmentalFactor(factor_name STRING PRIMARY KEY, description STRING, impact_level STRING)");

        conn.query("CREATE REL TABLE MigratesFrom(FROM BirdSpecies TO Location, departure_month INT64)");
        conn.query("CREATE REL TABLE MigratesTo(FROM BirdSpecies TO Location, arrival_month INT64)");
        conn.query("CREATE REL TABLE ActiveIn(FROM Location TO Season)");
        conn.query("CREATE REL TABLE InfluencedBy(FROM BirdSpecies TO EnvironmentalFactor, influence_strength INT64)");

        // Load data
        conn.query("COPY BirdSpecies FROM 'src/main/resources/migration/bird-species.csv'");
        conn.query("COPY Location FROM 'src/main/resources/migration/location.csv'");
        conn.query("COPY Season FROM 'src/main/resources/migration/season.csv'");
        conn.query("COPY EnvironmentalFactor FROM 'src/main/resources/migration/environmental-factor.csv'");
        conn.query("COPY MigratesFrom FROM 'src/main/resources/migration/migrates-from.csv'");
        conn.query("COPY MigratesTo FROM 'src/main/resources/migration/migrates-to.csv'");
        conn.query("COPY ActiveIn FROM 'src/main/resources/migration/active-in.csv'");
        conn.query("COPY InfluencedBy FROM 'src/main/resources/migration/influenced-by.csv'");

        // Query 1: All bird species and their migration distances
        System.out.println("\n=== Query 1: Bird Species and Migration Distances ===");
        QueryResult result = conn.query(
                "MATCH (b:BirdSpecies) " +
                "RETURN b.species_name, b.migration_distance, b.flight_duration " +
                "ORDER BY b.migration_distance DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 2: Complete migration routes (spring departure)
        System.out.println("\n=== Query 2: Spring Migration Routes (Departure) ===");
        result = conn.query(
                "MATCH (b:BirdSpecies)-[mf:MigratesFrom]->(origin:Location) " +
                "RETURN b.species_name, origin.location_name, mf.departure_month;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 3: Complete migration routes (fall arrival)
        System.out.println("\n=== Query 3: Fall Migration Routes (Arrival) ===");
        result = conn.query(
                "MATCH (b:BirdSpecies)-[mt:MigratesTo]->(destination:Location) " +
                "RETURN b.species_name, destination.location_name, mt.arrival_month;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 4: Long-distance migrants (>8000 miles)
        System.out.println("\n=== Query 4: Long-Distance Migrants (>8000 miles) ===");
        result = conn.query(
                "MATCH (b:BirdSpecies) WHERE b.migration_distance > 8000 " +
                "RETURN b.species_name, b.migration_distance, b.flight_duration " +
                "ORDER BY b.migration_distance DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 5: Locations used by multiple species
        System.out.println("\n=== Query 5: Key Stopover and Breeding Locations ===");
        result = conn.query(
                "MATCH (b:BirdSpecies)-[mf:MigratesFrom|MigratesTo]->(loc:Location) " +
                "WITH loc, COUNT(b) as species_count, COLLECT(b.species_name) as species_list " +
                "RETURN loc.location_name, loc.location_type, species_count, loc.habitat_quality " +
                "ORDER BY species_count DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 6: Environmental factors affecting migration
        System.out.println("\n=== Query 6: Environmental Factors Influencing Migration ===");
        result = conn.query(
                "MATCH (b:BirdSpecies)-[inf:InfluencedBy]->(ef:EnvironmentalFactor) " +
                "RETURN b.species_name, ef.factor_name, inf.influence_strength, ef.impact_level;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 7: Seasonal activity in locations
        System.out.println("\n=== Query 7: Locations and Their Active Seasons ===");
        result = conn.query(
                "MATCH (loc:Location)-[a:ActiveIn]->(season:Season) " +
                "RETURN loc.location_name, loc.location_type, season.season_name, season.month_range;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 8: Complete migration cycle analysis
        System.out.println("\n=== Query 8: Complete Migration Cycles ===");
        result = conn.query(
                "MATCH (b:BirdSpecies)-[mf:MigratesFrom]->(origin:Location), " +
                "(b)-[mt:MigratesTo]->(destination:Location) " +
                "RETURN b.species_name, origin.location_name, destination.location_name, " +
                "b.migration_distance, mf.departure_month, mt.arrival_month;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 9: Species with highest environmental sensitivity
        System.out.println("\n=== Query 9: Species Most Influenced by Environmental Factors ===");
        result = conn.query(
                "MATCH (b:BirdSpecies)-[inf:InfluencedBy]->(ef:EnvironmentalFactor) " +
                "WITH b, COUNT(ef) as factor_count, SUM(inf.influence_strength) as total_influence " +
                "RETURN b.species_name, b.migration_distance, factor_count, total_influence " +
                "ORDER BY total_influence DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 10: Migration efficiency (distance vs duration)
        System.out.println("\n=== Query 10: Migration Efficiency Analysis ===");
        result = conn.query(
                "MATCH (b:BirdSpecies) " +
                "RETURN b.species_name, b.migration_distance, b.flight_duration, " +
                "(b.migration_distance / b.flight_duration) as daily_distance " +
                "ORDER BY daily_distance DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // BIRD MIGRATION CYCLE DETECTION & ANALYSIS
        System.out.println("\n\n========== BIRD MIGRATION CYCLE DETECTION ==========");
        performMigrationCycleAnalysis(conn);
    }

    /**
     * Performs bird migration cycle detection and analysis
     */
    private static void performMigrationCycleAnalysis(Connection conn) {
        System.out.println("\n--- Analyzing Migratory Bird Patterns ---\n");

        // Query to get all bird species and their routes
        QueryResult speciesResult = conn.query(
                "MATCH (b:BirdSpecies)-[mf:MigratesFrom]->(origin:Location), " +
                "(b)-[mt:MigratesTo]->(destination:Location) " +
                "RETURN b.species_name, b.migration_distance, b.flight_duration, " +
                "origin.location_name, destination.location_name;");

        List<MigrationCycle> cycles = new ArrayList<>();

        while (speciesResult.hasNext()) {
            FlatTuple row = speciesResult.getNext();
            MigrationCycle cycle = new MigrationCycle(
                    row.getValue(0).toString(),
                    Double.parseDouble(row.getValue(1).toString()),
                    Integer.parseInt(row.getValue(2).toString()),
                    row.getValue(3).toString(),
                    row.getValue(4).toString()
            );
            cycles.add(cycle);
        }

        // Analyze and display cycles
        if (cycles.isEmpty()) {
            System.out.println("⚠️  No migration cycles found");
        } else {
            System.out.println("✅ " + cycles.size() + " ACTIVE MIGRATION CYCLE(S) DETECTED:\n");

            for (int i = 0; i < cycles.size(); i++) {
                MigrationCycle cycle = cycles.get(i);
                System.out.println("━".repeat(70));
                System.out.println("Cycle " + (i + 1) + ": " + cycle.species);
                System.out.println("━".repeat(70));
                System.out.println("🔄 Route: " + cycle.origin + " ↔ " + cycle.destination);
                System.out.println("📏 Distance: " + String.format("%.0f", cycle.distance) + " miles annually");
                System.out.println("⏱️  Duration: " + cycle.duration + " days total");
                System.out.println("📊 Daily Pace: " + String.format("%.0f", cycle.distance / cycle.duration) + " miles/day");

                // Classify migration type
                String migrationType;
                if (cycle.distance > 30000) {
                    migrationType = "🌍 EXTREME LONG-DISTANCE (Trans-Continental)";
                } else if (cycle.distance > 12000) {
                    migrationType = "📍 LONG-DISTANCE";
                } else if (cycle.distance > 6000) {
                    migrationType = "🛤️ MEDIUM-DISTANCE";
                } else {
                    migrationType = "🏘️ SHORT-DISTANCE";
                }
                System.out.println("Classification: " + migrationType);

                // Cycle health status
                double efficiency = cycle.distance / cycle.duration;
                String status;
                if (efficiency > 400) {
                    status = "🟢 OPTIMAL - Excellent migration efficiency";
                } else if (efficiency > 300) {
                    status = "🟡 NORMAL - Standard migration pace";
                } else {
                    status = "🔴 SLOW - Below normal pace, may indicate obstacles";
                }
                System.out.println("Status: " + status);
                System.out.println();
            }

            // Migration statistics
            System.out.println("\n" + "=".repeat(70));
            System.out.println("MIGRATION STATISTICS");
            System.out.println("=".repeat(70));

            double totalDistance = cycles.stream().mapToDouble(c -> c.distance).sum();
            double avgDistance = cycles.stream().mapToDouble(c -> c.distance).average().orElse(0);
            int maxDuration = cycles.stream().mapToInt(c -> c.duration).max().orElse(0);
            int minDuration = cycles.stream().mapToInt(c -> c.duration).min().orElse(0);

            System.out.println("Total Migratory Species: " + cycles.size());
            System.out.println("Combined Annual Distance: " + String.format("%.0f", totalDistance) + " miles");
            System.out.println("Average Migration Distance: " + String.format("%.0f", avgDistance) + " miles");
            System.out.println("Longest Migration Duration: " + maxDuration + " days");
            System.out.println("Shortest Migration Duration: " + minDuration + " days");

            // Identify extreme migrator
            MigrationCycle extremeMigrator = cycles.stream()
                    .max(Comparator.comparingDouble(c -> c.distance))
                    .orElse(null);
            if (extremeMigrator != null) {
                System.out.println("\n🏆 Champion Migrator: " + extremeMigrator.species);
                System.out.println("   Annual Journey: " + String.format("%.0f", extremeMigrator.distance) + " miles");
            }

            System.out.println("\n✅ All cycles are COMPLETE and FUNCTIONING NORMALLY");
        }
    }

    /**
     * Helper class to represent a bird migration cycle
     */
    private static class MigrationCycle {
        String species;
        double distance;
        int duration;
        String origin;
        String destination;

        MigrationCycle(String species, double distance, int duration, String origin, String destination) {
            this.species = species;
            this.distance = distance;
            this.duration = duration;
            this.origin = origin;
            this.destination = destination;
        }
    }
}

