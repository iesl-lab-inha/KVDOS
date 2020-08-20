package com.edge.cassandra;

import org.apache.log4j.BasicConfigurator;

import java.util.*;


public class KVDOS {


    public static int keyCounter = 1;
    private static final String[] senders = {"car1", "car2", "car3", "car4", "car5", "car6", "car7", "car8", "car9", "car10"};
    private static final String[] formats = {"string", "number", "binary", "sensor", "jpg", "png", "jpeg", "mp4"};
    private static final String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";
    public static final String delimiter = "_DDD_DDD_";


    public static void main(String[] args) {
        BasicConfigurator.configure();


        PreP prep = new PreP();
        prep.init();

        /*
        System.out.println("Testing KVI...");
        for(int i = 0; i < 10; i++){
            prep.insert(randomKey(), randomString(20));
        }

        System.out.println("Testing SDI...");
        for(int i = 0; i < 10; i++){
            int senderIndex = (int)(senders.length * Math.random());
            int formatIndex = (int)(formats.length * Math.random());

            prep.insert(senders[senderIndex], formats[formatIndex], randomString(20));
        }

        System.out.println("Testing MDI...");
        for(int i = 0; i < 10; i++){
            int senderIndex = (int)(senders.length * Math.random());
            int formatIndex = (int)(formats.length * Math.random());

            prep.insert(senders[senderIndex], formats[formatIndex], randomBytes(20));
        }
*/
        prep.close();

    }

    private static String randomString(int n)
    {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index = (int)(alphaNumericString.length() * Math.random());

            // add Character one by one in end of sb
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

    private static byte[] randomBytes(int n){
        byte[] arr = new byte[n];
        new Random().nextBytes(arr);
        return arr;
    }

    private static String randomKey()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("key").append(keyCounter++).append(delimiter);

        int senderIndex = (int)(senders.length * Math.random());
        int formatIndex = (int)(formats.length * Math.random());

        // add Character one by one in end of sb
        sb.append(senders[senderIndex]).append(delimiter)
        .append(formats[formatIndex]);

        return sb.toString();
    }



}
