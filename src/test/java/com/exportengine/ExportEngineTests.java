package com.exportengine;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import javax.sql.DataSource;

@SpringBootTest
class ExportEngineTests {

    @MockBean
    private DataSource dataSource;

    @Test
    void contextLoads() {
        // Verify that the Spring context loads successfully
    }
}
