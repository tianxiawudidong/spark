package com.ifchange.spark.bi.customer.portrait;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

public class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static String toJson(Object o) {
        if (o == null) {
            return null;
        } else if (o instanceof String) {
            return o.toString();
        }
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJson(String o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJsonDebug(Object o) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(String str, Class<T> clz) {
        try {

            return objectMapper.readValue(str, clz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> fromJson(String str, TypeReference<List<T>> typeReference, Class<T> clz) {
        try {
            return objectMapper.readValue(str, typeReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
