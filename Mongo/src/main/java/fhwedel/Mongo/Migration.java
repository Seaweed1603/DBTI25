/**
 * 
 * Nein, für MongoDB ist keine Normalform nötig.
 * Entitäten können Schemalos und geschachtelt dargestellt werden.
 * Es würde sich lohnen zu verschachteln:
 *  Personal in Abteilung
 *  Kinder in Personal
 *  mögl. Gehalt/Pramie in Personal
 * 
 */

/**
 * Konzept zur Darstellung der Firma-Datenbank in MongoDB
 * 
 * Abteilungen (`abteilungen`)
 * 
 *{
 * "abtnr": "d11",
 * "name": "Verwaltung"
 * "personal": [
 *   {
 *     "persnr": 123,
 *     "name": "Lehmann",
 *     "vorname": "Karl"
 *     "gehalt": 3500,
 *     "praemien": 0,
 *     "krankenkasse": "aok",
 *     "kinder": [
 *       { "k_name": "Lehmann", "k_vorname": "Sven" "k_geb": "2002" }
 *     ]
 *   },
 * ]
 *}
 * 
 * 
 * Maschine
 * {
 *  "mnr" : 1,
 *  "name": "Bohrmaschine",
 *  "pnr": 123,
 *  "ansch_datum": "1999-02-01",
 *  "neuwert": 30000,
 *  "zeitwert": 15000
 * }
 */

 package fhwedel.Mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Migration {

    private static final String JDBC_URL = "jdbc:mariadb://localhost:3306/firma";
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "password";

    private static final MongoClient mongoClient = MongoClients.create();
    private static final MongoDatabase mongoDB = mongoClient.getDatabase("firma_mongo");

    public static void main(String[] args) throws Exception {
        try (Connection mariaConn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {

            migrateAbteilungen(mariaConn);
            migrateMaschinen(mariaConn);

            System.out.println("Migration abgeschlossen.");
        }
    }

    private static void migrateAbteilungen(Connection conn) throws SQLException {
        MongoCollection<Document> abteilungen = mongoDB.getCollection("abteilungen");
        abteilungen.drop();

        // Abteilungen
        Map<String, Document> abtMap = new HashMap<>();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM abteilung")) {
            while (rs.next()) {
                String abtnr = rs.getString("abt_nr");
                String name = rs.getString("name");

                Document abtDoc = new Document("abtnr", abtnr)
                        .append("name", name)
                        .append("personal", new ArrayList<>());
                abtMap.put(abtnr, abtDoc);
            }
        }

        // Personal
        Map<Integer, Document> personalMap = new HashMap<>();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM personal")) {
            while (rs.next()) {
                int pnr = rs.getInt("pnr");
                String name = rs.getString("name");
                String vorname = rs.getString("vorname");
                String geh_stufe = rs.getString("geh_stufe");
                String abtnr = rs.getString("abt_nr");
                String krankenkasse = rs.getString("krankenkasse");

                Document persDoc = new Document("persnr", pnr)
                        .append("name", name)
                        .append("vorname", vorname)
                        .append("krankenkasse", krankenkasse)
                        .append("gehalt", 0)
                        .append("praemien", 0)
                        .append("kinder", new ArrayList<>());

                personalMap.put(pnr, persDoc);

                if (abtMap.containsKey(abtnr)) {
                    abtMap.get(abtnr).getList("personal", Document.class).add(persDoc);
                }
            }
        }

        // Gehalt
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM gehalt g join personal p where g.geh_stufe = p.geh_stufe")) {
            while (rs.next()) {
                int pnr = rs.getInt("pnr");
                double betrag = rs.getDouble("betrag");
                if (personalMap.containsKey(pnr)) {
                    personalMap.get(pnr).put("gehalt", betrag);
                }
            }
        }

        // Praemien
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM praemie")) {
            while (rs.next()) {
                int pnr = rs.getInt("pnr");
                double praemie = rs.getDouble("p_betrag");
                if (personalMap.containsKey(pnr)) {
                    personalMap.get(pnr).put("praemien", praemie);
                }
            }
        }

        // Kinder
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM kind")) {
            while (rs.next()) {
                int pnr = rs.getInt("pnr");
                String k_name = rs.getString("k_name");
                String k_vorname = rs.getString("k_vorname");
                String k_geb = rs.getString("K_geb");
                if (personalMap.containsKey(pnr)) {
                    List<Document> kinder = personalMap.get(pnr).getList("kinder", Document.class);
                    kinder.add(new Document("k_name", k_name).append("k_vorname", k_vorname).append("k_geb", k_geb));
                }
            }
        }

        abteilungen.insertMany(new ArrayList<>(abtMap.values()));
    }

    private static void migrateMaschinen(Connection conn) throws SQLException {
        MongoCollection<Document> maschinen = mongoDB.getCollection("maschinen");
        maschinen.drop();

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM maschine")) {
            List<Document> maschinenDocs = new ArrayList<>();
            while (rs.next()) {
                Document maschine = new Document()
                        .append("mnr", rs.getInt("mnr"))
                        .append("name", rs.getString("name"))
                        .append("pnr", rs.getInt("pnr"))
                        .append("ansch_datum", rs.getDate("ansch_datum").toString())
                        .append("neuwert", rs.getDouble("neuwert"))
                        .append("zeitwert", rs.getDouble("zeitwert"));
                maschinenDocs.add(maschine);
            }
            maschinen.insertMany(maschinenDocs);
        }
    }
}
