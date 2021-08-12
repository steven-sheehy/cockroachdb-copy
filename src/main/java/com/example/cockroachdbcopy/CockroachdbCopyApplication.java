package com.example.cockroachdbcopy;

import javax.sql.DataSource;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;


@SpringBootApplication
public class CockroachdbCopyApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(CockroachdbCopyApplication.class);

    @Value("${bufferSize:32768}")
    private int bufferSize;

    @Autowired
    private DataSource dataSource;

    public static void main(String[] args) {
        SpringApplication.run(CockroachdbCopyApplication.class, args);
    }


    @Bean
    ApplicationRunner runner() {
        return args -> {
            String csv = "8a3d70a3-d70a-4000-8000-00000000001d,seattle,Hannah,400 Broad St,0987654321\n";

            copy("COPY users FROM STDIN DELIMITER ',' NULL ''", csv);
            copy("COPY users FROM STDIN CSV", csv);
        };
    }

    private void copy(String sql, String data) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            truncate(connection);
            LOGGER.info("Executing SQL: {}", sql);
            LOGGER.info("Inserting data: {}", data);

            CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
            long count = copyManager.copyIn(sql, new StringReader(data), bufferSize);
            LOGGER.info("Inserted {} rows with buffer size {}", count, bufferSize);
            Assert.state(count == 1, "Expected 1 rows to be inserted but was actually " + count);
        }
    }

    private void truncate(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("truncate table users cascade")) {
            statement.executeUpdate();
        }
    }
}
