package com.github.serserser.kafka.etl.impl.serializers;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.junit.Test;

public class TestSerializingLimitedFields {

    @Test
    public void test() throws JsonProcessingException {
        Country country = new Country(123, "PL", "Polandia");
        SimpleBeanPropertyFilter beanFilter = SimpleBeanPropertyFilter.filterOutAllExcept("code", "name");
        SimpleFilterProvider filter = new SimpleFilterProvider().addFilter("filter", beanFilter);

        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writer(filter).writeValueAsString(country);
        System.out.println(jsonString);
    }

    @JsonFilter("filter")
    private class Country {

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
}
