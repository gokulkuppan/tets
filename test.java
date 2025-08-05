@Then("Compare SQL table {string} with Nested JSON response {string} and common key {string}")
public void compareSQLResultsWithNestedJsonResponse(String tableName, String jsonVarName, String commonKey) {
    DatasetResult sqlResult = (DatasetResult) getScenarioContext().getVariable(tableName);
    List<String> sqlHeaders = sqlResult.getHeader();
    List<List<Object>> sqlData = sqlResult.getData();

    Set<String> excludeColumns = new HashSet<>(Arrays.asList("RNK", "SNAPSHOT_PERIOD", "LOAD_TIME"));

    WebApiResult jsonResult = (WebApiResult) getScenarioContext().getVariable(jsonVarName);
    JsonPath jsonPath = jsonResult.getResponseBody().jsonPath();
    List<Map<String, Object>> jsonItems = jsonPath.getList("items");

    int commonKeyIndex = sqlHeaders.indexOf(commonKey);
    if (commonKeyIndex == -1) {
        throw new RuntimeException(commonKey + " column not found in SQL table");
    }

    // Build SQL lookup map
    Map<String, List<List<Object>>> sqlLookupMap = new HashMap<>();
    for (List<Object> row : sqlData) {
        Object id = row.get(commonKeyIndex);
        if (id != null) {
            String key = id.toString().trim();
            sqlLookupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
    }

    SoftAssert softAssert = new SoftAssert();
    List<Map<String, String>> comparisonResults = new ArrayList<>();

    for (Map<String, Object> jsonRow : jsonItems) {
        String jsonCommonKeyId = jsonRow.get(commonKey) != null ? jsonRow.get(commonKey).toString().trim() : null;
        Map<String, String> resultRow = new LinkedHashMap<>();
        resultRow.put(commonKey, jsonCommonKeyId);

        if (jsonCommonKeyId == null) {
            resultRow.put("Status", "Missing " + commonKey + " in Elastic Search");
            comparisonResults.add(resultRow);
            softAssert.fail("Elastic Search row missing: " + jsonRow);
            continue;
        }

        List<List<Object>> sqlRows = sqlLookupMap.get(jsonCommonKeyId);
        if (sqlRows == null) {
            resultRow.put("Status", "No matching SQL row found");
            comparisonResults.add(resultRow);
            softAssert.fail("No matching SQL row found for: " + jsonCommonKeyId);
            continue;
        }

        // Detect and handle nested fields dynamically
        for (Map.Entry<String, Object> entry : jsonRow.entrySet()) {
            String jsonKey = entry.getKey();
            Object jsonValue = entry.getValue();

            if (excludeColumns.contains(jsonKey)) continue;
            
            if (jsonValue instanceof List) {
                List<Map<String, Object>> nestedJsonList = (List<Map<String, Object>>) jsonValue;

                // Compare each nested JSON object to a matching SQL row
                for (int i = 0; i < nestedJsonList.size(); i++) {
                    Map<String, Object> nestedItem = nestedJsonList.get(i);

                    if (i >= sqlRows.size()) {
                        resultRow.put(jsonKey + "[" + i + "]", "Extra nested item in JSON not found in SQL");
                        softAssert.fail("Extra nested item in JSON for key: " + jsonCommonKeyId);
                        continue;
                    }

                    List<Object> sqlRow = sqlRows.get(i);

                    for (String nestedKey : nestedItem.keySet()) {
                        if (!sqlHeaders.contains(nestedKey)) {
                            resultRow.put(nestedKey, "Column not in SQL");
                            softAssert.fail("SQL missing nested column: " + nestedKey);
                            continue;
                        }

                        int colIndex = sqlHeaders.indexOf(nestedKey);
                        String sqlStr = sqlRow.get(colIndex) != null ? sqlRow.get(colIndex).toString().trim() : "";
                        String jsonStr = nestedItem.get(nestedKey) != null ? nestedItem.get(nestedKey).toString().trim() : "";

                        if (!sqlStr.equals(jsonStr)) {
                            resultRow.put(nestedKey + "[" + i + "]", "Mismatch SQL=" + sqlStr + ", Elastic=" + jsonStr);
                            softAssert.assertEquals(jsonStr, sqlStr,
                                    String.format("Mismatch for %s[%d], field '%s': SQL='%s', Elastic='%s'",
                                            jsonCommonKeyId, i, nestedKey, sqlStr, jsonStr));
                        } else {
                            resultRow.put(nestedKey + "[" + i + "]", "Match SQL=" + sqlStr + ", Elastic=" + jsonStr);
                        }
                    }
                }

            } else {
                // ✅ Fallback to original flat comparison
                if (!sqlHeaders.contains(jsonKey)) {
                    resultRow.put(jsonKey, "Column not in SQL");
                    softAssert.fail("SQL table does not contain column: " + jsonKey);
                    continue;
                }

                int sqlColIndex = sqlHeaders.indexOf(jsonKey);
                String sqlStr = sqlRows.get(0).get(sqlColIndex) != null ? sqlRows.get(0).get(sqlColIndex).toString().trim() : "";
                String jsonStr = jsonValue != null ? jsonValue.toString().trim() : "";

                if (!sqlStr.equals(jsonStr)) {
                    resultRow.put(jsonKey, "Mismatch SQL=" + sqlStr + ", Elastic=" + jsonStr);
                    softAssert.assertEquals(jsonStr, sqlStr,
                            String.format("Mismatch for %s, field '%s': SQL='%s', Elastic='%s'",
                                    jsonCommonKeyId, jsonKey, sqlStr, jsonStr));
                } else {
                    resultRow.put(jsonKey, "Match SQL=" + sqlStr + ", Elastic=" + jsonStr);
                }
            }
        }

        resultRow.put("Status", "Compared");
        comparisonResults.add(resultRow);
    }

    new ExcelReport().generateExcelReport(comparisonResults);
    softAssert.assertAll();
}
