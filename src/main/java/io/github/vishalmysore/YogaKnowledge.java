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

        // Create NODE tables
        conn.query("CREATE NODE TABLE YogaStyle(name STRING PRIMARY KEY, origin STRING, description STRING, difficulty_level INT64)");
        conn.query("CREATE NODE TABLE Pose(name STRING PRIMARY KEY, sanskrit_name STRING, difficulty INT64, description STRING, target_time STRING)");
        conn.query("CREATE NODE TABLE Benefit(name STRING PRIMARY KEY, category STRING, description STRING)");
        conn.query("CREATE NODE TABLE BodyPart(name STRING PRIMARY KEY, description STRING)");
        conn.query("CREATE NODE TABLE Instructor(name STRING PRIMARY KEY, experience_years INT64, specialization STRING, certification STRING)");
        conn.query("CREATE NODE TABLE Studio(name STRING PRIMARY KEY, city STRING, capacity INT64, opening_year INT64)");
        conn.query("CREATE NODE TABLE PoseType(name STRING PRIMARY KEY, description STRING)");

        // Create RELATIONSHIP tables
        conn.query("CREATE REL TABLE BelongsToStyle(FROM Pose TO YogaStyle)");
        conn.query("CREATE REL TABLE TargetsBenefit(FROM Pose TO Benefit, intensity INT64)");
        conn.query("CREATE REL TABLE EngagesBodyPart(FROM Pose TO BodyPart, engagement_level INT64)");
        conn.query("CREATE REL TABLE Teaches(FROM Instructor TO YogaStyle, years_teaching INT64)");
        conn.query("CREATE REL TABLE WorksAt(FROM Instructor TO Studio, start_year INT64)");
        conn.query("CREATE REL TABLE RecommendsFor(FROM YogaStyle TO Benefit)");
        conn.query("CREATE REL TABLE HasType(FROM Pose TO PoseType)");

        // Load data.
        conn.query("COPY YogaStyle FROM 'src/main/resources/yoga/yoga_style.csv'");
        conn.query("COPY Pose FROM 'src/main/resources/yoga/pose.csv'");
        conn.query("COPY Benefit FROM 'src/main/resources/yoga/benefit.csv'");
        conn.query("COPY BodyPart FROM 'src/main/resources/yoga/body_part.csv'");
        conn.query("COPY Instructor FROM 'src/main/resources/yoga/instructor.csv'");
        conn.query("COPY Studio FROM 'src/main/resources/yoga/studio.csv'");
        conn.query("COPY PoseType FROM 'src/main/resources/yoga/pose_type.csv'");
        conn.query("COPY BelongsToStyle FROM 'src/main/resources/yoga/belongs_to_style.csv'");
        conn.query("COPY TargetsBenefit FROM 'src/main/resources/yoga/targets_benefit.csv'");
        conn.query("COPY EngagesBodyPart FROM 'src/main/resources/yoga/engages_body_part.csv'");
        conn.query("COPY Teaches FROM 'src/main/resources/yoga/teaches.csv'");
        conn.query("COPY WorksAt FROM 'src/main/resources/yoga/works_at.csv'");
        conn.query("COPY RecommendsFor FROM 'src/main/resources/yoga/recommends_for.csv'");
        conn.query("COPY HasType FROM 'src/main/resources/yoga/has_type.csv'");

        // Query 1: All yoga styles and their poses
        System.out.println("\n=== Query 1: Yoga Styles and Their Poses ===");
        QueryResult result = conn.query(
                "MATCH (p:Pose)-[b:BelongsToStyle]->(s:YogaStyle) " +
                "RETURN s.name, p.name, p.sanskrit_name, p.difficulty ORDER BY s.name, p.difficulty;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 2: Benefits of each pose
        System.out.println("\n=== Query 2: Poses and Their Benefits ===");
        result = conn.query(
                "MATCH (p:Pose)-[t:TargetsBenefit]->(b:Benefit) " +
                "RETURN p.name, b.name, b.category, t.intensity " +
                "ORDER BY p.name, t.intensity DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 3: Body parts engaged by each pose
        System.out.println("\n=== Query 3: Poses and Body Parts Engaged ===");
        result = conn.query(
                "MATCH (p:Pose)-[e:EngagesBodyPart]->(bp:BodyPart) " +
                "RETURN p.name, bp.name, e.engagement_level " +
                "ORDER BY p.name, e.engagement_level DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 4: Advanced poses (difficulty >= 7)
        System.out.println("\n=== Query 4: Advanced Poses (Difficulty >= 7) ===");
        result = conn.query(
                "MATCH (p:Pose) WHERE p.difficulty >= 7 " +
                "RETURN p.name, p.sanskrit_name, p.difficulty " +
                "ORDER BY p.difficulty DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 5: Complete pose profile (style, benefits, body parts, types)
        System.out.println("\n=== Query 5: Complete Pose Profiles ===");
        result = conn.query(
                "MATCH (p:Pose)-[b:BelongsToStyle]->(s:YogaStyle), " +
                "(p)-[t:TargetsBenefit]->(ben:Benefit), " +
                "(p)-[e:EngagesBodyPart]->(bp:BodyPart), " +
                "(p)-[ht:HasType]->(pt:PoseType) " +
                "WHERE p.difficulty >= 5 " +
                "RETURN p.name, s.name, ben.name, bp.name, pt.name " +
                "ORDER BY p.name;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 6: Instructors and yoga styles they teach
        System.out.println("\n=== Query 6: Instructors and Yoga Styles They Teach ===");
        result = conn.query(
                "MATCH (i:Instructor)-[t:Teaches]->(s:YogaStyle) " +
                "RETURN i.name, i.specialization, s.name, t.years_teaching " +
                "ORDER BY i.name;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 7: Studios with their instructors
        System.out.println("\n=== Query 7: Studios with Instructors ===");
        result = conn.query(
                "MATCH (i:Instructor)-[w:WorksAt]->(st:Studio) " +
                "RETURN st.name, st.city, i.name, i.experience_years, w.start_year " +
                "ORDER BY st.name, i.experience_years DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 8: Yoga styles and their associated benefits
        System.out.println("\n=== Query 8: Yoga Styles and Associated Benefits ===");
        result = conn.query(
                "MATCH (s:YogaStyle)-[r:RecommendsFor]->(b:Benefit) " +
                "RETURN s.name, b.name, b.category " +
                "ORDER BY s.name;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 9: Beginner-friendly poses (difficulty <= 3)
        System.out.println("\n=== Query 9: Beginner Poses ===");
        result = conn.query(
                "MATCH (p:Pose)-[b:BelongsToStyle]->(s:YogaStyle) " +
                "WHERE p.difficulty <= 3 " +
                "RETURN p.name, s.name, p.difficulty " +
                "ORDER BY p.difficulty;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 10: Poses targeting flexibility
        System.out.println("\n=== Query 10: Poses for Flexibility Improvement ===");
        result = conn.query(
                "MATCH (p:Pose)-[t:TargetsBenefit]->(b:Benefit) " +
                "WHERE b.name = 'Flexibility' " +
                "WITH p, COUNT(DISTINCT b) as benefit_count " +
                "RETURN p.name, p.difficulty, benefit_count " +
                "ORDER BY benefit_count DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 11: Experienced instructors (>5 years)
        System.out.println("\n=== Query 11: Expert Instructors (>5 years) ===");
        result = conn.query(
                "MATCH (i:Instructor)-[t:Teaches]->(s:YogaStyle) " +
                "WHERE i.experience_years > 5 " +
                "RETURN i.name, i.experience_years, s.name, t.years_teaching " +
                "ORDER BY i.experience_years DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 12: Most engaged body parts
        System.out.println("\n=== Query 12: Most Engaged Body Parts ===");
        result = conn.query(
                "MATCH (p:Pose)-[e:EngagesBodyPart]->(bp:BodyPart) " +
                "WITH bp, COUNT(p) as pose_count, AVG(e.engagement_level) as avg_engagement " +
                "RETURN bp.name, pose_count, avg_engagement " +
                "ORDER BY pose_count DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 13: Pose types distribution
        System.out.println("\n=== Query 13: Pose Types Distribution ===");
        result = conn.query(
                "MATCH (p:Pose)-[ht:HasType]->(pt:PoseType) " +
                "WITH pt, COUNT(p) as pose_count " +
                "RETURN pt.name, pose_count " +
                "ORDER BY pose_count DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 14: Studio capacity analysis with instructors
        System.out.println("\n=== Query 14: Studio Capacity and Instructor Distribution ===");
        result = conn.query(
                "MATCH (i:Instructor)-[w:WorksAt]->(st:Studio) " +
                "WITH st, COUNT(i) as instructor_count, AVG(i.experience_years) as avg_experience " +
                "RETURN st.name, st.city, st.capacity, instructor_count, avg_experience " +
                "ORDER BY instructor_count DESC;");
        while (result.hasNext()) {
            System.out.println(result.getNext());
        }

        // Query 15: Most comprehensive pose analysis
        System.out.println("\n=== Query 15: Comprehensive Pose Analysis ===");
        result = conn.query(
                "MATCH (p:Pose)-[bs:BelongsToStyle]->(s:YogaStyle) " +
                "WITH p, s, COUNT(DISTINCT *) as relationship_count " +
                "RETURN p.name, p.sanskrit_name, s.name, p.difficulty, p.description " +
                "ORDER BY p.difficulty DESC LIMIT 10;");
       // while (result.hasNext()) {
         //   System.out.println(result.getNext());
      //  }
      //  QueryResult res = conn.query("DESCRIBE");
       // while (res.hasNext()) {
         //   System.out.println(res.getNext());
     //   }

    }

}
