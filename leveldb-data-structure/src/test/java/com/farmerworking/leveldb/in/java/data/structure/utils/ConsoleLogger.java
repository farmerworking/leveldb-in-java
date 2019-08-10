package com.farmerworking.leveldb.in.java.data.structure.utils;

import com.farmerworking.leveldb.in.java.api.Options;
import com.google.gson.Gson;

public class ConsoleLogger implements Options.Logger {
    @Override
    public void log(String msg, String... args) {
        if (args != null && args.length > 0) {
            System.out.println(msg + ", " + new Gson().toJson(args));
        } else {
            System.out.println(msg);
        }
    }
}
