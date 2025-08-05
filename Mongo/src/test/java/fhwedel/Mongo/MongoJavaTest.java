package fhwedel.Mongo;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;
import static org.junit.jupiter.api.Assertions.*;



/**
 * Die Tests sind so ausgelegt das sie nacheinander ausführen, mit: "mvn -Dtest=CRUDclientTest#TESTMETHODENNAME test" lassen sich die Tests einzeln
 * ausführen, da manche dinge redundant wären, rufen alle folgenden Tests die vorherigen aus um sicherzustellen das die DB richtig konfiguriert ist.
 * 
 * TestAufgabeC ruft TestAufgabeB auf, welche TestAufgabeA aufruft
 * 
 * 
 */
public class MongoJavaTest {
    private static MongoClient client;
    private static MongoDatabase db;
    private MongoCollection<Document> buch;
    private MongoCollection<Document> leser;
    private MongoCollection<Document> entliehen;

    @BeforeAll
    static void initClient() {
        client = MongoClients.create();
        MongoDatabase existingDb = client.getDatabase("bibliothek");
        for (String name : existingDb.listCollectionNames()) {
            existingDb.drop();
            break;
        }

        db = client.getDatabase("bibliothek");
    }

    @BeforeEach
    void setupCollections() {
        buch = db.getCollection("Buch");
        leser = db.getCollection("Leser");
        entliehen = db.getCollection("Entliehen");
    }

    @Test
    void testAufgabeA() {
        buch.insertOne(new Document("invnr", 1)
                .append("autor", "Marc-Uwe Kling")
                .append("titel", "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers")
                .append("verlag", "Ullstein-Verlag"));

        leser.insertOne(new Document("lnr", 1)
                .append("name", "Friedrich Funke")
                .append("adresse", "Bahnhofstraße 17, 23758 Oldenburg"));

        assertEquals(1, buch.countDocuments());
        assertEquals(1, leser.countDocuments());
    

    
        List<Document> books = Arrays.asList(
                new Document("invnr", 2).append("autor", "J.K. Rowling").append("titel", "Harry Potter und der Stein der Weisen").append("verlag", "PaulVerlag"),
                new Document("invnr", 3).append("autor", "Shrek").append("titel", "Mein Sumpf").append("verlag", "Ullstein"),
                new Document("invnr", 4).append("autor", "Horst Evers").append("titel", "Der König von Berlin").append("verlag", "Rowohlt-Verlag"),
                new Document("invnr", 5).append("autor", "Franz").append("titel", "Die Verwandlung").append("verlag", "PaulVerlag"),
                new Document("invnr", 6).append("autor", "Cornelia Funke").append("titel", "Tintenherz").append("verlag", "PaulVerlag")
        );
        buch.insertMany(books);

        List<Document> readers = Arrays.asList(
                new Document("lnr", 2).append("name", "Anna").append("adresse", "Lindenweg 5, 12345 Berlin"),
                new Document("lnr", 3).append("name", "Jonas").append("adresse", "Marktplatz 10, 12345 Mainz"),
                new Document("lnr", 4).append("name", "Laura").append("adresse", "Rosenstraße 3, 12345 Köln"),
                new Document("lnr", 5).append("name", "Peter").append("adresse", "Musterstraße 12, 12345 Hamburg"),
                new Document("lnr", 6).append("name", "Julia").append("adresse", "Hauptstraße 1, 12345 München")
        );
        leser.insertMany(readers);

        assertEquals(6, buch.countDocuments());
        assertEquals(6, leser.countDocuments());

        entliehen.insertMany(Arrays.asList(
                new Document("ivnr", 3).append("lnr", 2),
                new Document("ivnr", 2).append("lnr", 4),
                new Document("ivnr", 4).append("lnr", 4),
                new Document("ivnr", 4).append("lnr", 2)
        ));
    }

    @Test
    void testAufgabeB() {
        testAufgabeA();

        var cursor = buch.find(eq("autor", "Marc-Uwe Kling"));
        assertTrue(cursor.iterator().hasNext());
    }

    @Test
    void testAufgabeC() {
        testAufgabeB();

        assertEquals(6, buch.countDocuments());
    }

    @Test
    void testAufgabeD() {
        testAufgabeC();
        
        AggregateIterable<Document> result = entliehen.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$lnr").append("anzahl", new Document("$sum", 1))),
                new Document("$match", new Document("anzahl", new Document("$gt", 1))),
                new Document("$sort", new Document("anzahl", -1)),
                new Document("$lookup", new Document("from", "Leser").append("localField", "_id").append("foreignField", "lnr").append("as", "leser")),
                new Document("$unwind", "$leser"),
                new Document("$project", new Document("_id", 0).append("name", "$leser.name").append("anzahl", 1))
        ));

        assertTrue(result.iterator().hasNext());
    }

    @Test
    void testAufgabeE() {
        testAufgabeD();

        entliehen.insertOne(new Document("ivnr", 1).append("lnr", 1).append("rueckgabedatum", new java.util.Date()));
        assertEquals(1, entliehen.countDocuments(eq("lnr", 1)));
        entliehen.deleteOne(eq("ivnr", 1));
        assertEquals(0, entliehen.countDocuments(eq("ivnr", 1)));
    }

    @Test
    void testAufgabeF_G() {
        testAufgabeE();

        Document heinz = new Document("lnr", 7)
                .append("name", "Heinz Müller")
                .append("adresse", "Klopstockweg 17, 38124 Braunschweig")
                .append("ausgeliehen", Arrays.asList(
                        new Document("invnr", 1).append("titel", "Die Känguru-Chroniken").append("rueckgabedatum", new java.util.Date()),
                        new Document("invnr", 4).append("titel", "Der König von Berlin").append("rueckgabedatum", new java.util.Date())
                ));
        leser.insertOne(heinz);

        //ENTFERNEN
        leser.updateOne(eq("lnr", 7), pull("ausgeliehen", new Document("invnr", 1)));
        Document updated = leser.find(eq("lnr", 7)).first();
        assertNotNull(updated);
        assertEquals(1, ((List<?>) updated.get("ausgeliehen")).size());

        //Hinzufügen
        leser.insertOne(new Document("lnr", 1).append("name","Friedrich Funke").append("adresse","...").append("ausgeliehen", List.of()));
        leser.updateOne(eq("lnr", 1), push("ausgeliehen", new Document("invnr",1).append("titel","Die Känguru-Chroniken").append("rueckgabedatum", new java.util.Date())));
        Document funke = leser.find(eq("lnr",1)).first();
        assertEquals(1, ((List<?>) funke.get("ausgeliehen")).size());
    }
}