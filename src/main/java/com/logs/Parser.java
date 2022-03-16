package com.logs;

import java.util.*;

public class Parser {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter a log: ");
        String[] auditLog = sc.nextLine().split("\"");
//        for(String p : auditLog) {
//            System.out.println("=>"+p);
//        }
        ArrayList<String> auditLogParams = new ArrayList<>();
        String[] firstAuditLog = auditLog[0].split(" ");
        firstAuditLog[2] = firstAuditLog[2] + " " + firstAuditLog[3];
        for(int i = 0; i < firstAuditLog.length; i++) {
            if(i != 3) {
                auditLogParams.add(firstAuditLog[i].trim());
            }
        }
        auditLogParams.add("\"" + auditLog[1].trim() + "\"");
        String[] thirdAuditLog = auditLog[2].trim().split(" ");
        for(String x : thirdAuditLog) {
            auditLogParams.add(x.trim());
        }
        auditLogParams.add("\"" + auditLog[3].trim() + "\"");
        auditLogParams.add("\"" + auditLog[5].trim() + "\"");
        String[] seventhAuditLog = auditLog[6].substring(0, auditLog[6].length() - 1).trim().split(" ");
        for(String x : seventhAuditLog) {
            auditLogParams.add(x.trim());
        }
//        for(String p : auditLogParams) {
//            System.out.println(p);
//        }
        ArrayList<String> auditLogParamsNames = new ArrayList<>(Arrays.asList("bucketOwner", "bucket", "time", "remoteIp",
                "arn", "requestId", "operation", "key", "requestUri", "httpStatus", "errorCode", "bytesSent",
                "objectSize", "totalTime", "turnaroundTime", "httpReferrerHeader", "userAgent", "versionId",
                "hostId", "signatureVersion", "cipherSuite", "authenticationType", "hostHeader", "tlsVersion"));
        Map<String, String> mp = new HashMap<>();
        for(int i = 0; i < auditLogParamsNames.size(); i++) {
            mp.put(auditLogParamsNames.get(i), auditLogParams.get(i));
        }
//        System.out.println(mp);
//        System.out.println(mp.size());
        String referrerHeader = mp.get("httpReferrerHeader");
        //System.out.println(referrerHeader);
        int idx = referrerHeader.indexOf("?");
        //System.out.println(idx);
        referrerHeader = referrerHeader.substring(idx + 1, referrerHeader.length() - 1);
        //System.out.println(referrerHeader);
        String[] httpReferrerParams = referrerHeader.split("&");
//        for(String p : httpReferrerParams) {
//            System.out.println(p);
//        }
        for (String p : httpReferrerParams) {
            String[] p1 = p.split("=");
            mp.put(p1[0], p1[1]);
        }
        System.out.println(mp);
        //System.out.println(mp.size());
    }
}


