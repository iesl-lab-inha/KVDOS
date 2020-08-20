package com.edge.cassandra;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class PreP {

    /***
     * Type of data:
     * 1: Key-Value data
     * 2: Sensor data
     * 3: Media data
    **/
    private int type;
    private final boolean critical = false;

    private static final String keySpace = "edgekeyspace";
    private static final String table = "edgetable";
    private static CassandraDB db = null;

    //192.168.1.16

    public void init(){
        db = new CassandraDB();
        db.connect("10.106.196.182", 30043);
        db.createKeyspace(keySpace, "SimpleStrategy", 3);

        Map<String, String> fields = new HashMap<String, String>();
        for(int i = 0; i < Item.fieldTypes.length; i++){
            fields.put(Item.fieldNames[i], Item.fieldTypes[i]);
        }

        db.createTable(keySpace, table, fields, 1);

    }
    public void close(){
        db.close();
    }


    public void insert(String key, String value){
        KVI(key, value);
    }

    public void insert(String sender, String format, String value){
        SDI(sender, format, value);
    }

    public void insert(String sender, String format, byte[] value){
        MDI(sender, format, value);
    }

    public void KVI(String key, String value){
        String[] keywords = key.split(KVDOS.delimiter);
        String sender = keywords[1];
        String format = keywords[2];
        this.type = 1;

        Item item = new Item(key, value, sender, format, this.type, new Date(), critical, null);
        db.insertItem(keySpace, table, item);

    }

    public void SDI(String sender, String format, String value){
        StringBuilder sb = new StringBuilder();
        sb.append("key").append(KVDOS.keyCounter++).append(KVDOS.delimiter).append(sender).append(KVDOS.delimiter).append(format);
        KVI(sb.toString(), value);
    }

    public void MDI(String sender, String format, byte[] value){
        //SDI(sender, format, escapeMetaCharacters(new String(value)));

        StringBuilder sb = new StringBuilder();
        sb.append("key").append(KVDOS.keyCounter++).append(KVDOS.delimiter).append(sender).append(KVDOS.delimiter).append(format);

        Item item = new Item(sb.toString(), null, sender, format, this.type, new Date(), critical, value);
        db.insertItem(keySpace, table, item);
    }



}
