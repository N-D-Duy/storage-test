package com.duynguyen.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import lombok.Getter;

@Getter
public class Config {
    @Getter
    private static final Config instance = new Config();
    @Getter
    public String azureConnectionString;

    public boolean load(){
        try{
            FileInputStream input = new FileInputStream("config.properties");
            Properties props = new Properties();
            props.load(new InputStreamReader(input, StandardCharsets.UTF_8));
            props.forEach((t, u) -> {
                Log.info(String.format("Config - %s: %s", t, u));
            });
            azureConnectionString = props.getProperty("AZURE_BLOB_STRING");
        } catch (Exception e){
            Log.error("Config file not found");
            return false;
        }
        return true;
    }
}

