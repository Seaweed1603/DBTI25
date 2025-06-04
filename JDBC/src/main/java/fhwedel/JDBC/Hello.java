package fhwedel.JDBC;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Hello {
    private static final String CONNECTION_STRING = "jbbc:mariadb://localhost:3306/firma";

    public static void main(String[] args) {
        Connection connection = connectToDB(CONNECTION_STRING);
        if (connection == null) {
            return;
        }
    }
    private static connection connectToDB(String connection_string) {
        try {
            Connection connection = DriverManager.getConnection(connection_string);
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank konnte nicht hergestellt werden!");
            return null;
        }
        
    }
}
