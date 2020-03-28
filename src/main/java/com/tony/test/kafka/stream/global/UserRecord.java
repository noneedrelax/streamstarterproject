package com.tony.test.kafka.stream.global;

public class UserRecord {
    String name;
    String gender;
    String email;

    public UserRecord(String name, String gender, String email) {
        this.name = name;
        this.gender = gender;
        this.email = email;
    }
}
