package fhwedel.JDBC;

import java.sql.*;

public class Hello {
    /**
     * Database Connection Vars
     */
    private static final String CONNECTION_URL = "jdbc:mariadb://localhost:3306/firma";
    private static final String CONNECTION_USER = "root";
    private static final String CONNECTION_PASSWORD = "password";
    private static Connection connectionDB = null;
    private static Statement statement = null;

    public static void main(String[] args) {
        try {
            connectionDB = connectToDB(CONNECTION_URL, CONNECTION_USER, CONNECTION_PASSWORD);
        } catch (SQLException e) {
            System.err.println(e);
            return;
        }
        System.out.println("Connection Established!");
        try {
          statement = getStatement();  
        } catch (SQLException e) {
            System.err.println(e);
            return;
        }
        
        try {
            create(statement);
        } catch (SQLException e) {
            System.err.println(e);
        }

        try {
            read(statement);
        } catch (SQLException e) {
            System.err.println(e);
        }
        
        try {
            update(statement);
        } catch (SQLException e) {
            System.err.println(e);
        }
        
        try {
            delete(statement);
        } catch (SQLException e) {
            System.err.println(e);
        }

        try {
            readFemaleSales(statement);
        } catch (SQLException e) {
            System.err.println(e);
        }










    }

    /**
     * Function to Connect to a Database.
     * 
     * @param url URL to the DB
     * @param user Username for Login
     * @param password Password for Login
     * 
     * @return Connection to DB if successfull.
     * 
     * @throws {@link SQLException} if Usernam/Password is incorrect or the Connection failed.
     */
    private static Connection connectToDB(String url, String user, String password) throws SQLException {
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            return connection;

        } catch (SQLTimeoutException e) {
            throw new SQLException("Timeout!\n" + e);
        }catch (SQLException e) {
            throw new SQLException("Verbindung zur Datenbank konnte nicht hergestellt werden!\n" + e);
        }
    }

    /**
     * Handler for sending requests to the DB.
     * 
     * @param connection Connection Handler
     * @return Statement for accepting sql request
     * 
     * @throws SQLException If a database access error occurs or is called on a closed connection
     */
    private static Statement getStatement() throws SQLException {
        try {
            Statement st = connectionDB.createStatement();
            return st;
        } catch (SQLException e) {
            throw new SQLException("Erstellen des Statement-Handlers fehlgeschlagen!\n" + e);
        }
    }

    /**
     * Inserts a Statement into personal.
     * 
     * @param st The object used for executing a static SQL statement and returning the results it produces.
     * 
     * @throws SQLException if a database access error occurs
     */
    private static void create(Statement st) throws SQLException {
        try {
            st.executeUpdate("INSERT INTO personal VALUES ('417', 'Hendrik', 'Krause', 'it1', 'd13', 'tkk')");
        } catch (SQLException e) {
            throw new SQLException("Create Fehlgeschlagen!\n" + e);
        }
    }

    /**
     * Reads all entries from personal.
     * 
     * @param st The object used for executing a static SQL statement and returning the results it produces.
     * 
     * @throws SQLException if a database access error occurs
     */
    private static final void read(Statement st) throws SQLException {
        ResultSet result = st.executeQuery("SELECT * FROM personal");
        boolean headerprinted = false;
        System.out.println("\n\n\n");
        while(result.next()) {
            int columns = result.getMetaData().getColumnCount();
            for (int i = 1; !headerprinted && i <= columns; i++) {
                System.out.print(result.getMetaData().getColumnLabel(i) + " ");
            }
            System.out.println();
            headerprinted = true;
            for (int i = 1; i <= columns; i++) {
                System.out.print(result.getString(i) + " ");
            }
        }
        System.out.println("\n\n\n");
    }

    /**
     * Updates a row in the DB.
     * 
     * @param st The object used for executing a static SQL statement and returning the results it produces.
     * 
     * @throws SQLException if a database access error occurs
     */
    private static void update(Statement st) throws SQLException {
        try {
            st.executeUpdate("update gehalt set betrag = betrag * 1.1 where geh_stufe = 'it1'");
        } catch (SQLException e) {
            throw new SQLException("Create Fehlgeschlagen!\n" + e);
        }
    }

    /**
     * Deletes a row from DB.
     * 
     * @param st The object used for executing a static SQL statement and returning the results it produces.
     * 
     * @throws SQLException if a database access error occurs
     */
    private static void delete(Statement st) throws SQLException {
        try {
            st.executeUpdate("delete from personal where pnr = 135");
        } catch (SQLException e) {
            throw new SQLException("Create Fehlgeschlagen!\n" + e);
        }
    }

    /**
     * Prints all Sales a associates.
     * 
     * @param st The object used for executing a static SQL statement and returning the results it produces.
     * 
     * @throws SQLException if a database access error occurs
     */
    private static final void readFemaleSales(Statement st) throws SQLException {
        ResultSet result = st.executeQuery("select * from personal p left join abteilung a on a.abt_nr = p.abt_nr where a.name = 'Verkauf'");
        boolean headerprinted = false;
        System.out.println("\n\n\n");
        while(result.next()) {
            int columns = result.getMetaData().getColumnCount();
            for (int i = 1; !headerprinted && i <= columns; i++) {
                System.out.print(result.getMetaData().getColumnLabel(i) + " ");
            }
            System.out.println();
            headerprinted = true;
            for (int i = 1; i <= columns; i++) {
                System.out.print(result.getString(i) + " ");
            }
        }
        System.out.println("\n\n\n");
    }

    
}
