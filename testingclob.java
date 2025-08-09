public String clobToString(Clob clob) throws Exception {
    if (clob == null) return null;

    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(clob.getCharacterStream()))) {
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
    }
    return sb.toString();
}
