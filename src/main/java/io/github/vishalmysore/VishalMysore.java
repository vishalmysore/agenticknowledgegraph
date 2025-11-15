package io.github.vishalmysore;

import com.kuzudb.Database;
import com.kuzudb.FlatTuple;
import com.kuzudb.QueryResult;
import com.kuzudb.*;
import java.util.*;

public class VishalMysore {

    public static void main(String[] args) {
        // Create an empty on-disk database and connect to it
        Database db = new Database("vishal_mysore.kuzu");
        Connection conn = new Connection(db);

        // Create tables for person knowledge graph
        conn.query("CREATE NODE TABLE Person(person_name STRING PRIMARY KEY, description STRING)");
        conn.query("CREATE NODE TABLE Location(location_name STRING PRIMARY KEY, region STRING, description STRING)");
        conn.query("CREATE NODE TABLE Organization(org_name STRING PRIMARY KEY, industry STRING, description STRING)");
        conn.query("CREATE NODE TABLE Skill(skill_name STRING PRIMARY KEY, category STRING, description STRING)");
        conn.query("CREATE NODE TABLE Achievement(achievement_name STRING PRIMARY KEY, description STRING)");

        conn.query("CREATE REL TABLE HasSkill(FROM Person TO Skill, proficiency_level INT64)");
        conn.query("CREATE REL TABLE WorksFor(FROM Person TO Organization, years_of_service INT64)");
        conn.query("CREATE REL TABLE LivesIn(FROM Person TO Location)");
        conn.query("CREATE REL TABLE BornIn(FROM Person TO Location)");
        conn.query("CREATE REL TABLE Has(FROM Person TO Achievement, count INT64)");
        conn.query("CREATE REL TABLE Located(FROM Organization TO Location)");
        conn.query("CREATE REL TABLE RelatedTo(FROM Skill TO Skill, relationship_type STRING)");

        // Load data
        conn.query("COPY Person FROM 'src/main/resources/vishal/person.csv'");
        conn.query("COPY Location FROM 'src/main/resources/vishal/location.csv'");
        conn.query("COPY Organization FROM 'src/main/resources/vishal/organization.csv'");
        conn.query("COPY Skill FROM 'src/main/resources/vishal/skill.csv'");
        conn.query("COPY Achievement FROM 'src/main/resources/vishal/achievement.csv'");
        conn.query("COPY HasSkill FROM 'src/main/resources/vishal/has-skill.csv'");
        conn.query("COPY WorksFor FROM 'src/main/resources/vishal/works-for.csv'");
        conn.query("COPY LivesIn FROM 'src/main/resources/vishal/lives-in.csv'");
        conn.query("COPY BornIn FROM 'src/main/resources/vishal/born-in.csv'");
        conn.query("COPY Has FROM 'src/main/resources/vishal/has-achievement.csv'");
        conn.query("COPY Located FROM 'src/main/resources/vishal/located.csv'");
        conn.query("COPY RelatedTo FROM 'src/main/resources/vishal/related-to.csv'");

        // Query 1: Person profile
        System.out.println("\n=== Query 1: Vishal Mysore Profile ===");
        QueryResult result = conn.query(
                "MATCH (p:Person) WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN p.person_name, p.description;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 2: All skills and proficiency levels
        System.out.println("\n=== Query 2: Skills and Proficiency Levels ===");
        result = conn.query(
                "MATCH (p:Person)-[hs:HasSkill]->(s:Skill) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN s.skill_name, s.category, hs.proficiency_level " +
                "ORDER BY hs.proficiency_level DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 3: Technical skills hierarchy
        System.out.println("\n=== Query 3: Technical Skills and Related Technologies ===");
        result = conn.query(
                "MATCH (s1:Skill)-[r:RelatedTo]->(s2:Skill) " +
                "RETURN s1.skill_name, r.relationship_type, s2.skill_name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 4: Experience and organizations
        System.out.println("\n=== Query 4: Work Experience ===");
        result = conn.query(
                "MATCH (p:Person)-[wf:WorksFor]->(o:Organization) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN o.org_name, o.industry, wf.years_of_service;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 5: Locations and residency
        System.out.println("\n=== Query 5: Geographic Presence ===");
        result = conn.query(
                "MATCH (p:Person)-[li:LivesIn]->(loc:Location) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN loc.location_name, loc.region, loc.description;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 6: Achievements
        System.out.println("\n=== Query 6: Achievements and Awards ===");
        result = conn.query(
                "MATCH (p:Person)-[h:Has]->(a:Achievement) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN a.achievement_name, h.count, a.description;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 7: Skills by category
        System.out.println("\n=== Query 7: Skills Grouped by Category ===");
        result = conn.query(
                "MATCH (p:Person)-[hs:HasSkill]->(s:Skill) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "WITH s.category as category, COLLECT(s.skill_name) as skills, AVG(hs.proficiency_level) as avg_proficiency " +
                "RETURN category, skills, avg_proficiency " +
                "ORDER BY avg_proficiency DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 8: Organization locations
        System.out.println("\n=== Query 8: Organizations and Their Locations ===");
        result = conn.query(
                "MATCH (o:Organization)-[loc:Located]->(l:Location) " +
                "RETURN o.org_name, o.industry, l.location_name, l.region;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 9: Complete professional profile
        System.out.println("\n=== Query 9: Complete Professional Profile ===");
        result = conn.query(
                "MATCH (p:Person) WHERE p.person_name = 'Vishal Mysore' " +
                "MATCH (p)-[hs:HasSkill]->(s:Skill) " +
                "WITH p, COUNT(s) as total_skills " +
                "MATCH (p)-[wf:WorksFor]->(o:Organization) " +
                "WITH p, total_skills, COUNT(o) as total_organizations, SUM(wf.years_of_service) as total_experience " +
                "MATCH (p)-[h:Has]->(a:Achievement) " +
                "WITH p, total_skills, total_organizations, total_experience, SUM(h.count) as total_achievements " +
                "RETURN p.person_name, total_skills, total_organizations, total_experience, total_achievements;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // Query 10: Advanced analytics - Skill proficiency analysis
        System.out.println("\n=== Query 10: Skill Proficiency Analysis ===");
        result = conn.query(
                "MATCH (p:Person)-[hs:HasSkill]->(s:Skill) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "WITH s.category as category, COUNT(*) as skill_count, AVG(hs.proficiency_level) as avg_proficiency " +
                "RETURN category, skill_count, avg_proficiency, " +
                "CASE WHEN avg_proficiency >= 90 THEN 'Expert' WHEN avg_proficiency >= 75 THEN 'Advanced' WHEN avg_proficiency >= 60 THEN 'Intermediate' ELSE 'Beginner' END as proficiency_level " +
                "ORDER BY avg_proficiency DESC;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.println(row);
        }

        // PROFESSIONAL PROFILE ANALYSIS
        System.out.println("\n\n========== PROFESSIONAL PROFILE SUMMARY ==========");
        performProfessionalAnalysis(conn);
    }

    /**
     * Performs comprehensive professional analysis and profile generation
     */
    private static void performProfessionalAnalysis(Connection conn) {
        System.out.println("\n--- Analyzing Professional Profile ---\n");

        // Query total skills
        QueryResult skillResult = conn.query(
                "MATCH (p:Person)-[hs:HasSkill]->(s:Skill) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN COUNT(s) as skill_count, AVG(hs.proficiency_level) as avg_proficiency;");

        int skillCount = 0;
        double avgProficiency = 0;

        if (skillResult.hasNext()) {
            FlatTuple row = skillResult.getNext();
            skillCount = Integer.parseInt(row.getValue(0).toString());
            avgProficiency = Double.parseDouble(row.getValue(1).toString());
        }

        // Query organizations
        QueryResult orgResult = conn.query(
                "MATCH (p:Person)-[wf:WorksFor]->(o:Organization) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN o.org_name, wf.years_of_service;");

        List<String> organizations = new ArrayList<>();
        int totalExperience = 0;

        while (orgResult.hasNext()) {
            FlatTuple row = orgResult.getNext();
            organizations.add(row.getValue(0).toString());
            totalExperience += Integer.parseInt(row.getValue(1).toString());
        }

        // Query locations
        QueryResult locResult = conn.query(
                "MATCH (p:Person)-[li:LivesIn]->(loc:Location) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN loc.location_name, loc.region;");

        List<String> locations = new ArrayList<>();
        while (locResult.hasNext()) {
            FlatTuple row = locResult.getNext();
            locations.add(row.getValue(0).toString());
        }

        // Query achievements
        QueryResult achieveResult = conn.query(
                "MATCH (p:Person)-[h:Has]->(a:Achievement) " +
                "WHERE p.person_name = 'Vishal Mysore' " +
                "RETURN a.achievement_name, h.count;");

        int totalAchievements = 0;
        while (achieveResult.hasNext()) {
            FlatTuple row = achieveResult.getNext();
            totalAchievements += Integer.parseInt(row.getValue(1).toString());
        }

        // Print analysis
        System.out.println("👤 VISHAL MYSORE - PROFESSIONAL PROFILE");
        System.out.println("=".repeat(70));
        System.out.println("\n📊 KEY METRICS:");
        System.out.println("  • Total Skills: " + skillCount);
        System.out.println("  • Average Proficiency: " + String.format("%.1f%%", avgProficiency));
        System.out.println("  • Years of Experience: " + totalExperience);
        System.out.println("  • Organizations: " + organizations.size());
        System.out.println("  • Achievements: " + totalAchievements);
        System.out.println("  • Geographic Presence: " + locations.size() + " locations");

        System.out.println("\n🏢 ORGANIZATIONS:");
        for (String org : organizations) {
            System.out.println("  ✓ " + org);
        }

        System.out.println("\n📍 LOCATIONS:");
        for (String loc : locations) {
            System.out.println("  ✓ " + loc);
        }

        // Proficiency classification
        String classification;
        if (avgProficiency >= 90) {
            classification = "🌟 EXPERT LEVEL";
        } else if (avgProficiency >= 80) {
            classification = "⭐ ADVANCED LEVEL";
        } else if (avgProficiency >= 70) {
            classification = "📈 INTERMEDIATE-ADVANCED";
        } else {
            classification = "📚 INTERMEDIATE LEVEL";
        }

        System.out.println("\n🎯 OVERALL ASSESSMENT: " + classification);
        System.out.println("\nProfile Status: ✅ COMPLETE AND COMPREHENSIVE");
        System.out.println("=".repeat(70));
    }
}

