package com.jayden.study.query;

import com.jayden.study.utils.JdbcUtils;

import java.sql.*;
import java.time.Clock;
import java.time.Duration;

import static java.time.Clock.systemUTC;

/**
 * Count Record
 *
 * @author jayden-lee
 */
public class CountRecord {
    private static final String url = "jdbc:mysql://localhost:3306/mysql";
    private static final String user = "root";
    private static final String password = "";

    private static Connection connection;

    public static void main(String[] args) throws Exception {
        connection = JdbcUtils.getConnection(url, user, password);
        truncatePersons();
        injectPersons();
        countPersons();
        createTemporaryIdsTable();
        injectIdsIntTemporaryTable();
        countTemporaryIds();
        printSelectedRows();
    }

    private static void truncatePersons() throws SQLException {
        connection.prepareStatement("TRUNCATE persons").execute();
    }

    private static void countTemporaryIds() throws SQLException {
        ResultSet resultSet = connection.prepareStatement("SELECT count(*) from temporary_person_ids").executeQuery();
        resultSet.next();
        int temporaryIdsCount = resultSet.getInt(1);
        System.out.println("Temporary person ids injected in the temporary table: " + temporaryIdsCount );
    }

    private static void countPersons() throws SQLException {
        ResultSet resultSet = connection.prepareStatement("SELECT count(*) from persons ").executeQuery();
        resultSet.next();
        int count = resultSet.getInt(1);
        System.out.println(count + " persons in the main table");
    }

    private static void injectPersons() throws SQLException {
        long millisStart = systemUTC().millis();
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO PERSONS(id) VALUES(?)");
        int n = 94000;
        for (int i = 0; i < n; i++) {
            preparedStatement.setString(1, "" + i);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        Duration duration = Duration.ofMillis(systemUTC().millis() - millisStart);
        System.out.println(duration.dividedBy(n));
    }

    private static void injectIdsIntTemporaryTable() throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO temporary_person_ids (id) VALUES (?)");
        for (int i = 0; i < 91000; i++) {
            preparedStatement.setString(1, ""+i);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
    }

    private static void createTemporaryIdsTable() throws SQLException {
        String sql = "CREATE TEMPORARY TABLE temporary_person_ids (id text);";
        connection.prepareStatement(sql).execute();
    }

    private static int printSelectedRows() throws SQLException {
        String sql = "SELECT count(*) from persons where id in (select id from temporary_person_ids)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int selectedRows = resultSet.getInt(1);
        System.out.println("Total Rows : " + selectedRows);
        return selectedRows;
    }
}
