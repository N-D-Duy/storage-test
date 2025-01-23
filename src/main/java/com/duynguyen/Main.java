package com.duynguyen;

import com.duynguyen.utils.Config;
import com.duynguyen.utils.Log;

public class Main {
    public static void main(String[] args) {
        if(Config.getInstance().load()){
            StorageWebSocketServer server = new StorageWebSocketServer(8020, Config.getInstance().getAzureConnectionString());
            server.start();
        } else {
            Log.error("Failed to load config");
        }

    }
}