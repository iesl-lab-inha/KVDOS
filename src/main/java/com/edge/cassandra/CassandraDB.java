package com.edge.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraDB {

    private static Cluster cluster;

    private static Session session;

    //10.244.2.91

    private String defaultIP = "127.0.0.1";

    public void connect() {
        Cluster.Builder b = Cluster.builder().addContactPoint(defaultIP);
        cluster = b.build();
        session = cluster.connect();
    }


    public void connect(String nodeIP, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(nodeIP).withPort(port);
        cluster = b.build();
        session = cluster.connect();
    }


    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");
        String query = sb.toString();
        session.execute(query);
        System.out.println("Keyspace Created: " + keyspaceName);
    }

    public void createTable(String keySpace, String table, Map<String, String> fields, int primaryIndex) {
        int counter = 1;
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keySpace).append(".").append(table).append("(");

        for(Map.Entry<String, String> me : fields.entrySet()){
            if(counter == primaryIndex){
                sb.append(me.getKey()).append(" ").append(me.getValue()).append(" PRIMARY KEY,");
            }else{
                sb.append(me.getKey()).append(" ").append(me.getValue()).append(",");
            }
            counter++;
        }
        sb.append(");");

        String query = sb.toString();
        session.execute(query);
        System.out.println("Table Created: " + keySpace + "." + table);
    }

    public void insertItem(String keyspace, String tableName, Item item) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(keyspace).append(".").append(tableName).append("(")
                .append(Item.fieldNames[0]).append(", ")
                .append(Item.fieldNames[1]).append(", ")
                .append(Item.fieldNames[2]).append(", ")
                .append(Item.fieldNames[3]).append(", ")
                .append(Item.fieldNames[4]).append(", ")
                .append(Item.fieldNames[5]).append(", ")
                .append(Item.fieldNames[6]).append(", ")
                .append(Item.fieldNames[7]).append(") ")
                .append("VALUES ('")
                .append(item.getKey()).append("', '")
                .append(item.getValue()).append("', '")
                .append(item.getSender()).append("', '")
                .append(item.getFormat()).append("', ")
                .append(item.getType()).append(", ")
                .append("toTimeStamp(now()), ");
                if(item.getValueBlob() == null){
                    sb.append(item.getCritical()).append(", ")
                    .append("null);");
                }else {
                    sb.append(item.getCritical()).append(", textAsBlob('")
                    .append(escapeMetaCharacters(new String(item.getValueBlob()))).append("'));");
                }
        String query = sb.toString();

        System.out.println("Query: " + query);

        session.execute(query);
        System.out.println("Item inserted...");
    }

    public Item getItem(String keySpace, String tableName, String ID) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(keySpace).append(".").append(tableName).append(" WHERE ")
                .append(Item.fieldNames[0]).append(" = '").append(ID).append("';");

        String query = sb.toString();
        ResultSet rs = session.execute(query);
        Item item = null;
        Row r;
        if((r = rs.one()) != null){
            item = new Item(r.getString(Item.fieldNames[0]), r.getString(Item.fieldNames[1]), r.getString(Item.fieldNames[2]),
                    r.getString(Item.fieldNames[3]), r.getInt(Item.fieldNames[4]), r.getTimestamp(Item.fieldNames[5]),
                    r.getBool(Item.fieldNames[6]), null);
        }

        return item;
    }

    public List<Item> getAll(String keySpace, String tableName) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(keySpace).append(".").append(tableName).append(";");

        String query = sb.toString();
        ResultSet rs = session.execute(query);
        List<Item> items = new ArrayList<Item>();
        List<Row> rows = rs.all();
        for (Row r : rows) {
            items.add(new Item(r.getString(Item.fieldNames[0]), r.getString(Item.fieldNames[1]), r.getString(Item.fieldNames[2]),
                    r.getString(Item.fieldNames[3]), r.getInt(Item.fieldNames[4]), r.getTimestamp(Item.fieldNames[5]),
                    r.getBool(Item.fieldNames[6]), null));
        }
        return items;
    }

    public void delete(String keySpace, String tableName, String ID){
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(keySpace).append(".").append(tableName).append(" WHERE ")
                .append(Item.fieldNames[0]).append(" = '").append(ID).append("';");

        String query = sb.toString();
        session.execute(query);
    }

    public String escapeMetaCharacters(String inputString){
        final String[] metaCharacters = {"\\","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&","%"};

        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                inputString = inputString.replace(metaCharacters[i],"\\"+metaCharacters[i]);
            }
        }
        return inputString;
    }


    /*
    public void update(String tableName, Item item){
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(tableName).append(" WHERE ")
                .append(Item.getKeyField()).append(" = \"").append(ID).append("\";");

        String query = sb.toString();
        session.execute(query);
    }*/

}
