package com.zmyuan.bg.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ImpalaJDBCDemo {

    private static final String SQL_STATEMENT = "SELECT * FROM customers limit 10";
    private static final String IMPALAD_HOST = "192.168.137.133";
    private static final String IMPALAD_JDBC_PORT = "21050";
    private static final String
            CONNECTION_URL =
            "jdbc:impala://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/default;auth=noSasl";
    private static final String
            CONNECTION_FILE_URL =
            "jdbc:impala://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/default;auth=noSasl";

    private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

    public static void main(String[] args) {
        testQuery();


    }

    public static void testInsert() {
        // insert into t1 partition(x=10, y='a')

        Connection  con = null;
        try {
            con = getFileConnection();
            con.createStatement().executeUpdate("insert into t1 partition(x=10, y='a') VALUE ()");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if(con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void testQuery() {
        System.out.println("\n=============================================");
        System.out.println("Cloudera Impala JDBC Example");
        System.out.println("Using Connection URL: " + CONNECTION_URL);
        System.out.println("Running Query: " + SQL_STATEMENT);

        Connection con = null;

        try {
            con = getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(SQL_STATEMENT);
            System.out.println("\n== Begin Query Results ======================");
            // print the results to the console
            int columnCount = rs.getMetaData().getColumnCount();
            for(int i=1; i<= columnCount; i++) {
                System.out.print(rs.getMetaData().getColumnName(i) + "\t\t");
            }
            System.out.println("\t");
            System.out.println("-------------------------------------------");
            while (rs.next()) {
                for(int i=1; i<=columnCount; i++) {
                    System.out.print(rs.getObject(i) + "\t");
                }
                System.out.println("\t");
            }

            System.out.println("== End Query Results =======================\n\n");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(con != null){
                    con.close();
                }
            } catch (Exception e) {
            }
        }
    }

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER_NAME);
        return DriverManager.getConnection(CONNECTION_URL);
    }
    public static Connection getFileConnection() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER_NAME);
        return DriverManager.getConnection(CONNECTION_FILE_URL);
    }
}