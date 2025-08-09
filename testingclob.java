List<String> sqlValues = Arrays.stream(sqlConcatValue.split(";"))
            .map(String::trim) // keep "ID - Name"
            .toList();

    // JSON side
    List<String> jsonValues = nestedJsonList.stream()
            .map(nestedItem -> {
                String idKey = nestedItem.keySet().stream()
                        .filter(k -> k.endsWith("_ID"))
                        .findFirst()
                        .orElse(null);
                String nameKey = nestedItem.keySet().stream()
                        .filter(k -> k.endsWith("_NAME"))
                        .findFirst()
                        .orElse(null);

                Object id = idKey != null ? nestedItem.get(idKey) : null;
                Object name = nameKey != null ? nestedItem.get(nameKey) : null;

                return (id != null && name != null)
                        ? id.toString().trim() + " - " + name.toString().trim()
                        : "";
            })
            .toList();
