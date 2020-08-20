package com.edge.cassandra;

import java.sql.Blob;
import java.util.Date;

public class Item {
    private String key;
    private String value;
    private String sender;
    private String format;
    private int type;
    private Date time;
    private Boolean critical;
    private byte[] valueBlob;

    public static String[] fieldNames = {"id_key", "data_value", "sender", "format", "type", "r_time", "critical", "data_blob"};
    public static String[] fieldTypes = {"text", "text", "text", "text", "int", "timestamp", "Boolean", "blob"};

    public Item(String key, String value) {
        this.key = key;
        this.value = value;
        this.sender = null;
        this.format = null;
        this.type = 0;
        this.time = null;
        this.critical = false;
        this.valueBlob = null;
    }


    public Item(String key, String value, String sender, String format, int type, Date time, Boolean critical, byte[] valueBlob) {
        this.key = key;
        this.value = value;
        this.sender = sender;
        this.type = type;
        this.format = format;
        this.time = time;
        this.critical = critical;
        this.valueBlob = valueBlob;
    }

    public void setValueBlob(byte[] valueBlob) {
        this.valueBlob = valueBlob;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public void setCritical(Boolean critical) {
        this.critical = critical;
    }

    public byte[] getValueBlob() {
        return valueBlob;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getSender() {
        return sender;
    }

    public int getType() {
        return type;
    }

    public String getFormat() {
        return format;
    }

    public Date getTime() {
        return time;
    }

    public Boolean getCritical() {
        return critical;
    }

    @Override
    public String toString() {
        return "Item{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", sender='" + sender + '\'' +
                ", format='" + format + '\'' +
                ", type=" + type +
                ", time=" + time +
                ", critical=" + critical +
                '}';
    }
}
