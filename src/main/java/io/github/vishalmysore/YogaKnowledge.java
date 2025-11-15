package io.github.vishalmysore;

import com.kuzudb.Database;
import com.kuzudb.FlatTuple;
import com.kuzudb.QueryResult;
import com.kuzudb.*;

public class YogaKnowledge {

    public static void main(String[] args)  {
        // Create an empty on-disk database and connect to it
        Database db = new Database("yoga.kuzu");
        Connection conn = new Connection(db);
        // Create tables.
        conn.query("CREATE NODE TABLE Instructor(name STRING PRIMARY KEY, experience_years INT64)");
        conn.query("CREATE NODE TABLE Pose(name STRING PRIMARY KEY, difficulty_level INT64)");
        conn.query("CREATE NODE TABLE Studio(name STRING PRIMARY KEY, capacity INT64)");
        conn.query("CREATE REL TABLE Teaches(FROM Instructor TO Pose, frequency INT64)");
        conn.query("CREATE REL TABLE WorksAt(FROM Instructor TO Studio)");
        // Load data.
        conn.query("COPY Instructor FROM 'src/main/resources/yoga/instructor.csv'");
        conn.query("COPY Pose FROM 'src/main/resources/yoga/pose.csv'");
        conn.query("COPY Studio FROM 'src/main/resources/yoga/studio.csv'");
        conn.query("COPY Teaches FROM 'src/main/resources/yoga/teaches.csv'");
        conn.query("COPY WorksAt FROM 'src/main/resources/yoga/works-at.csv'");

        // Query 1: Simple - All instructors and poses they teach
        System.out.println("\n=== Query 1: Instructors and Poses They Teach ===");
        QueryResult result =
                conn.query("MATCH (i:Instructor)-[t:Teaches]->(p:Pose) RETURN i.name, t.frequency, p.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 2: Find instructors with their studios and poses (3-way relationship)
        System.out.println("\n=== Query 2: Instructors with Their Studios and Poses ===");
        result = conn.query(
                "MATCH (i:Instructor)-[wa:WorksAt]->(s:Studio), (i)-[t:Teaches]->(p:Pose) " +
                "RETURN i.name, s.name, p.name, t.frequency;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 3: Find all instructors with high experience (> 8 years)
        System.out.println("\n=== Query 3: Experienced Instructors (>8 years) ===");
        result = conn.query(
                "MATCH (i:Instructor) WHERE i.experience_years > 8 " +
                "RETURN i.name, i.experience_years ORDER BY i.experience_years DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 4: Find difficult poses (difficulty level >= 4)
        System.out.println("\n=== Query 4: Difficult Poses (difficulty >= 4) ===");
        result = conn.query(
                "MATCH (p:Pose) WHERE p.difficulty_level >= 4 " +
                "RETURN p.name, p.difficulty_level ORDER BY p.difficulty_level DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 5: Find instructors teaching difficult poses
        System.out.println("\n=== Query 5: Instructors Teaching Difficult Poses (>=4) ===");
        result = conn.query(
                "MATCH (i:Instructor)-[t:Teaches]->(p:Pose) WHERE p.difficulty_level >= 4 " +
                "RETURN i.name, i.experience_years, p.name, p.difficulty_level, t.frequency " +
                "ORDER BY i.experience_years DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 6: Studios with their instructors and total teaching frequency
        System.out.println("\n=== Query 6: Studios with Instructors and Their Teaching Load ===");
        result = conn.query(
                "MATCH (i:Instructor)-[wa:WorksAt]->(s:Studio), (i)-[t:Teaches]->(p:Pose) " +
                "RETURN s.name, i.name, COUNT(p) as poses_taught, SUM(t.frequency) as total_weekly_frequency " +
                "ORDER BY s.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 7: Find instructors who teach more than 2 poses
        System.out.println("\n=== Query 7: Instructors Teaching Multiple Poses (>2) ===");
        result = conn.query(
                "MATCH (i:Instructor)-[t:Teaches]->(p:Pose) " +
                "WITH i, COUNT(p) as pose_count " +
                "WHERE pose_count > 2 " +
                "MATCH (i)-[t2:Teaches]->(p2:Pose) " +
                "RETURN i.name, pose_count, p2.name " +
                "ORDER BY i.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 8: Find the most frequently taught poses
        System.out.println("\n=== Query 8: Most Frequently Taught Poses ===");
        result = conn.query(
                "MATCH (i:Instructor)-[t:Teaches]->(p:Pose) " +
                "RETURN p.name, p.difficulty_level, SUM(t.frequency) as total_weekly_sessions " +
                "ORDER BY total_weekly_sessions DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 9: Find instructors with highest teaching load
        System.out.println("\n=== Query 9: Instructors with Highest Teaching Load ===");
        result = conn.query(
                "MATCH (i:Instructor)-[t:Teaches]->(p:Pose) " +
                "WITH i, SUM(t.frequency) as total_frequency, COUNT(p) as pose_count " +
                "RETURN i.name, i.experience_years, pose_count, total_frequency " +
                "ORDER BY total_frequency DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 10: Complex - Studios ranked by instructor experience level
        System.out.println("\n=== Query 10: Studios Ranked by Average Instructor Experience ===");
        result = conn.query(
                "MATCH (i:Instructor)-[wa:WorksAt]->(s:Studio) " +
                "WITH s, AVG(i.experience_years) as avg_experience, COUNT(i) as instructor_count " +
                "RETURN s.name, s.capacity, instructor_count, avg_experience " +
                "ORDER BY avg_experience DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }
    }

}
