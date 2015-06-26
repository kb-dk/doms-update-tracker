package dk.statsbiblioteket.doms.updatetracker.improved.worklog;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Simple class found at http://www.coderanch.com/t/306966/JDBC/databases/Execute-sql-file-java
 * Used to create the update tracker logs table and content in the integration tests
 */
public class Database {

    private static final String DRIVER_NAME = "org.postgresql.Driver";
    private static String url;
    private static String user;
    private static String password;

    static {
        try {
            Class.forName(DRIVER_NAME).newInstance();
            System.out.println("*** Driver loaded");
        } catch (Exception e) {
            System.out.println("*** Error : " + e.toString());
            System.out.println("*** ");
            System.out.println("*** Error : ");
            e.printStackTrace();
        }

    }


    public static void init(String url, String user, String password){

        Database.url = url;
        Database.user = user;
        Database.password = password;
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static void executeSQL(File sqlFile) throws SQLException, IOException {
        StringBuilder sb = new StringBuilder();

        FileReader fr = new FileReader(sqlFile);
        // be sure to not have line starting with "--" or "/*" or any other non aplhabetical character

        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();

        // here is our splitter ! We use ";" as a delimiter for each request
        // then we are sure to have well formed statements
        String[] inst = sb.toString().split(";");

        Connection c = Database.getConnection();
        Statement st = c.createStatement();

        for (String anInst : inst) {
            // we ensure that there is no spaces before or after the request string
            // in order to not execute empty statements
            if (!anInst.trim().equals("")) {
                st.executeUpdate(anInst);
                System.out.println(">>" + anInst);
            }
        }
    }
}