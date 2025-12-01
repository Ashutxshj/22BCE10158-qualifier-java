package com.bajajfinserv.qualifier;

import com.fasterxml.jackson.databind.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Component
public class StartupRunner implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        System.out.println("=== Application Started ===");

        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();

        String generateUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("name", "Ashutosh Jha"); // CHANGE THIS
        requestBody.put("regNo", "22BCE10158"); // CHANGE THIS
        requestBody.put("email", "ashutosh06066@gmail.com"); // CHANGE THIS

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

        System.out.println("Sending POST request to generate webhook...");
        ResponseEntity<String> response = restTemplate.postForEntity(generateUrl, request, String.class);

        System.out.println("Response: " + response.getBody());

        JsonNode jsonNode = mapper.readTree(response.getBody());
        String webhookUrl = jsonNode.get("webhook").asText();
        String accessToken = jsonNode.get("accessToken").asText();

        System.out.println("Webhook URL: " + webhookUrl);
        System.out.println("Access Token: " + accessToken);

        String regNo = requestBody.get("regNo");
        int lastTwoDigits = Integer.parseInt(regNo.substring(regNo.length() - 2));

        String sqlQuery;
        if (lastTwoDigits % 2 == 1) {
            System.out.println("RegNo ends in ODD - Using Question 1 SQL");
            sqlQuery = getSqlQuestion1();
        } else {
            System.out.println("RegNo ends in EVEN - Using Question 2 SQL");
            sqlQuery = getSqlQuestion2();
        }

        System.out.println("Final SQL Query: " + sqlQuery);

        String submitUrl = "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA";

        Map<String, String> solution = new HashMap<>();
        solution.put("finalQuery", sqlQuery);

        HttpHeaders submitHeaders = new HttpHeaders();
        submitHeaders.setContentType(MediaType.APPLICATION_JSON);
        submitHeaders.set("Authorization", accessToken);

        HttpEntity<Map<String, String>> submitRequest = new HttpEntity<>(solution, submitHeaders);

        System.out.println("Submitting solution...");
        ResponseEntity<String> submitResponse = restTemplate.postForEntity(submitUrl, submitRequest, String.class);

        System.out.println("Submission Response: " + submitResponse.getBody());
        System.out.println("=== Process Complete ===");
    }

    private String getSqlQuestion1() {
        return "SELECT * FROM table_name WHERE condition = 'value'";
    }

    private String getSqlQuestion2() {
        return """
                               WITH HighEarningEmployees AS (
                    SELECT DISTINCT
                        E.EMP_ID,
                        E.DEPARTMENT AS DEPARTMENT_ID,
                        E.FIRST_NAME,
                        E.LAST_NAME,
                        TIMESTAMPDIFF(YEAR, E.DOB, CURDATE()) AS AGE
                    FROM
                        EMPLOYEE E
                    INNER JOIN
                        PAYMENTS P ON E.EMP_ID = P.EMP_ID
                    WHERE
                        P.AMOUNT > 70000.00
                ),
                DepartmentStats AS (
                    SELECT
                        DEPARTMENT_ID,
                        AGE,
                        CONCAT(FIRST_NAME, ' ', LAST_NAME) AS FULL_NAME,
                        AVG(AGE) OVER (PARTITION BY DEPARTMENT_ID) AS AVERAGE_AGE,
                        ROW_NUMBER() OVER (PARTITION BY DEPARTMENT_ID ORDER BY EMP_ID) as rn
                    FROM
                        HighEarningEmployees
                )
                SELECT
                    D.DEPARTMENT_NAME,
                    T1.AVERAGE_AGE,
                    GROUP_CONCAT(
                        CASE WHEN T1.rn <= 10 THEN T1.FULL_NAME ELSE NULL END
                        ORDER BY T1.rn
                        SEPARATOR ', '
                    ) AS EMPLOYEE_LIST
                FROM
                    DEPARTMENT D
                INNER JOIN
                    DepartmentStats T1 ON D.DEPARTMENT_ID = T1.DEPARTMENT_ID
                GROUP BY
                    D.DEPARTMENT_NAME, D.DEPARTMENT_ID, T1.AVERAGE_AGE
                ORDER BY
                    D.DEPARTMENT_ID DESC;
                                """;

    }
}