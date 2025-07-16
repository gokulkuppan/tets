for (int i = 0; i < jsonItems.size(); i++) {
        Map<String, Object> jsonRow = jsonItems.get(i);
        List<Object> sqlRow = sqlData.get(i);

        for (String jsonKey : jsonRow.keySet()) {
            if (!sqlHeaders.contains(jsonKey)) {
                continue; // Skip keys not present in SQL
            }

            int sqlColIndex = sqlHeaders.indexOf(jsonKey);
            Object sqlValue = sqlRow.get(sqlColIndex);
            Object jsonValue = jsonRow.get(jsonKey);

            // Normalize both to String
            String sqlStr = sqlValue != null ? sqlValue.toString().trim() : "";
            String jsonStr = jsonValue != null ? jsonValue.toString().trim() : "";

            Assert.assertEquals(
                jsonStr, sqlStr,
                String.format("Mismatch at row %d, field '%s': SQL='%s', JSON='%s'", i + 1, jsonKey, sqlStr, jsonStr)
            );
        }
    }
