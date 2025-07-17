// Find index of GAU_CONTROL_ID in SQL headers
    int controlIdIndex = sqlHeaders.indexOf("GAU_CONTROL_ID");
    if (controlIdIndex == -1) {
        throw new RuntimeException("GAU_CONTROL_ID column not found in SQL table");
    }

    // Build SQL lookup map: { GAU_CONTROL_ID -> SQL Row }
    Map<String, List<Object>> sqlLookupMap = new HashMap<>();
    for (List<Object> row : sqlData) {
        Object controlId = row.get(controlIdIndex);
        if (controlId != null) {
            sqlLookupMap.put(controlId.toString().trim(), row);
        }
    }

    // Compare JSON with SQL row-by-row using GAU_CONTROL_ID
    for (Map<String, Object> jsonRow : jsonItems) {
        String jsonControlId = jsonRow.get("GAU_CONTROL_ID") != null ? jsonRow.get("GAU_CONTROL_ID").toString().trim() : null;
        if (jsonControlId == null) {
            Assert.fail("JSON row missing GAU_CONTROL_ID: " + jsonRow);
            continue;
        }

        List<Object> sqlRow = sqlLookupMap.get(jsonControlId);
        if (sqlRow == null) {
            Assert.fail("No matching SQL row found for GAU_CONTROL_ID: " + jsonControlId);
            continue;
        }

        // Compare each JSON key with SQL if present in SQL headers
        for (String jsonKey : jsonRow.keySet()) {
            if (!sqlHeaders.contains(jsonKey)) {
                continue; // Skip if SQL doesn't have this column
            }

            int sqlColIndex = sqlHeaders.indexOf(jsonKey);
            Object sqlValue = sqlRow.get(sqlColIndex);
            Object jsonValue = jsonRow.get(jsonKey);

            // Normalize
            String sqlStr = sqlValue != null ? sqlValue.toString().trim() : "";
            String jsonStr = jsonValue != null ? jsonValue.toString().trim() : "";

            Assert.assertEquals(
                jsonStr, sqlStr,
                String.format("Mismatch for GAU_CONTROL_ID '%s', field '%s': SQL='%s', JSON='%s'",
                        jsonControlId, jsonKey, sqlStr, jsonStr)
            );
        }
    }
