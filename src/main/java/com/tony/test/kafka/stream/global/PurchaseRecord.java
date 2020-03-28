package com.tony.test.kafka.stream.global;

public class PurchaseRecord {
    String name;
    String item;
    double price;

    public PurchaseRecord(String name, String item, double price) {
        this.name = name;
        this.item = item;
        this.price = price;
    }
}
