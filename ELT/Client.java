import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class Client {

    public static int traitementIntegerValue(Object nb) {
        if(nb instanceof String) {
            return 0;
        } 
        return Integer.valueOf(nb.toString());
    }
    public static String traitementSexe(String sexe) {
        if(sexe.equalsIgnoreCase("M")) {
            sexe = "Mâle";
        } 
        if(sexe.equalsIgnoreCase("F")) {
            sexe = "Femelle";
        }
        return sexe;
    }

    public static void main(String[] args) {
        // MongoDB connection parameters
        String mongoHost = "localhost";
        int mongoPort = 27017;
        String mongoDatabaseName = "voiture";
        String mongoCollectionName = "client";

        // Hive connection parameters
        String hiveJdbcUrl = "jdbc:hive2://localhost:10000/default";
        String hiveUserName = "vagrant";
        String hivePassword = "";

        // Connect to MongoDB
        MongoClient mongoClient = new MongoClient(mongoHost, mongoPort);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(mongoCollectionName);

        // Connect to Hive
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection hiveConnection = DriverManager.getConnection(hiveJdbcUrl, hiveUserName, hivePassword);
            System.out.println("connection to hive done");
            Statement stmt = hiveConnection.createStatement();
            MongoCursor<Document> cursor = collection.find().iterator();
            stmt.executeUpdate("DROP TABLE IF EXISTS client");
            System.out.println("drop table client if exists");
            String createTableSQL = "CREATE TABLE client ("+
               "age INT,"+
                "sexe STRING,"+
                "taux DOUBLE,"+
                "situationFamiliale STRING,"+
                "nbEnfantsAcharge INT,"+
                "deuxiemeVoiture STRING,"+
                "immatriculation STRING )";
            stmt.executeUpdate(createTableSQL);
            int i= 0;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                int age = traitementIntegerValue(doc.get("age"));
                String sexe = traitementSexe(doc.get("sexe").toString());
                int taux = traitementIntegerValue(doc.get("taux"));
                String situationFamiliale = doc.get("situationFamiliale").toString(); 
                int nbEnfantsAcharge = traitementIntegerValue(doc.get("nbEnfantsAcharge"));
                String deuxiemeVoiture = doc.get("2eme voiture").toString(); 
                String immatriculation = doc.get("immatriculation").toString(); 
                String insertQuery = "INSERT INTO TABLE client VALUES (" +
                                        age + ", '" +
                                        sexe + "', " +
                                        taux + ", '" +
                                        situationFamiliale + "', " +
                                        nbEnfantsAcharge + ", '" +
                                        deuxiemeVoiture + "', '" +
                                        immatriculation + "')";
                stmt.executeUpdate(insertQuery);
                i++;
                System.out.println("Insert data " + i + " successfully");
            }

            // Close connections
            cursor.close();
            stmt.close();
            hiveConnection.close();
            mongoClient.close();

            System.out.println("Data transfer from MongoDB to Hive completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
   
}