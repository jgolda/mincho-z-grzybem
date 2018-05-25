package com.github.serserser.kafka.etl.impl.data;

public class Country {

    private Integer id;
    private String code;
    private String name;

    public Country(Integer id, String code, String name) {
        this.id = id;
        this.code = code;
        this.name = name;
    }

    public Country() {
    }

    public Integer getId() {
        return id;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
