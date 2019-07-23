package com.farmerworking.leveldb.in.java.file.impl;

import com.farmerworking.leveldb.in.java.api.Options;
import com.google.gson.Gson;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

import java.lang.reflect.Field;

public class LogImpl implements Options.Logger{
    private final Gson gson;
    private org.slf4j.Logger logger;

    public LogImpl(String name) {
        this.logger = LoggerFactory.getLogger(name);
        this.gson = new Gson();

        try {
            if (this.logger instanceof Log4jLoggerAdapter) {
                Log4jLoggerAdapter adapter = (Log4jLoggerAdapter) this.logger;
                Field field = Log4jLoggerAdapter.class.getDeclaredField("logger");
                field.setAccessible(true);

                Logger log4jLogger = (Logger) field.get(adapter);
                log4jLogger.removeAllAppenders();

                FileAppender appender = new FileAppender();
                appender.setName(name);
                appender.setFile(name);
                appender.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
                appender.setThreshold(Level.INFO);
                appender.setAppend(false);
                appender.activateOptions();

                log4jLogger.addAppender(appender);
            }
        } catch (Exception e) {
            throw new RuntimeException("create logger error", e);
        }
    }

    @Override
    public void log(String msg, String... args) {
        this.logger.info(String.format("%s, args: %s", msg, gson.toJson(args)));
    }
}
