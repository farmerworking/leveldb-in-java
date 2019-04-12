package com.farmerworking.leveldb.in.java.common;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class Status {
    private Integer code;
    private String message;

    public Status() {
        this.code = Code.kOk.value;
    }

    private Status(Code code, String msg1, String msg2) {
        this.code = code.value;
        this.message = msg1 + ": " + msg2;
    }

    private Status(Code code, String msg) {
        this.code = code.value;
        this.message = msg;
    }

    public Status(Status status) {
        this.code = status.code;
        this.message = status.message;
    }

    // Returns true iff the status indicates success.
    public boolean isOk() { return (code.equals(Code.kOk.value)); }

    public boolean isNotOk() { return (!code.equals(Code.kOk.value)); }

    // Returns true iff the status indicates a NotFound error.
    public boolean IsNotFound() { return code.equals(Code.kNotFound.value); }

    // Returns true iff the status indicates a Corruption error.
    public boolean IsCorruption() { return code.equals(Code.kCorruption.value); }

    // Returns true iff the status indicates an IOError.
    public boolean IsIOError() { return code.equals(Code.kIOError.value); }

    // Returns true iff the status indicates a NotSupportedError.
    public boolean IsNotSupportedError() { return code.equals(Code.kNotSupported.value); }

    // Returns true iff the status indicates an InvalidArgument.
    public boolean IsInvalidArgument() { return code.equals(Code.kInvalidArgument.value); }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    @Override
    public String toString() {
        String result;

        Code valueOf = Code.valueOf(code);
        if (valueOf == null) {
            result = String.format("Unknown code(%d)", code);
        } else {
            result = valueOf.display;
        }

        return result + (StringUtils.isEmpty(message) ? "" : ": " + message);
    }

    public static Status OK() {
        return new Status();
    }

    public static Status NotFound(String msg1) {
        return new Status(Code.kNotFound, msg1);
    }

    public static Status Corruption(String msg1) {
        return new Status(Code.kCorruption, msg1);
    }

    public static Status NotSupported(String msg) {
        return new Status(Code.kNotSupported, msg);
    }

    public static Status InvalidArgument(String msg) {
        return new Status(Code.kInvalidArgument, msg);
    }

    public static Status IOError(String msg) {
        return new Status(Code.kIOError, msg);
    }

    public static Status NotFound(String msg1, String msg2) {
        return new Status(Code.kNotFound, msg1, msg2);
    }

    public static Status Corruption(String msg1, String msg2) {
        return new Status(Code.kCorruption, msg1, msg2);
    }

    public static Status NotSupported(String msg, String msg2) {
        return new Status(Code.kNotSupported, msg, msg2);
    }

    public static Status InvalidArgument(String msg, String msg2) {
        return new Status(Code.kInvalidArgument, msg, msg2);
    }

    public static Status IOError(String msg, String msg2) {
        return new Status(Code.kIOError, msg, msg2);
    }

    enum Code {
        kOk(0, "OK"),
        kNotFound(1, "NotFound"),
        kCorruption(2, "Corruption"),
        kNotSupported(3, "Not implemented"),
        kInvalidArgument(4, "Invalid argument"),
        kIOError(5, "IO error");

        private static Map<Integer, Code> map = new HashMap<>();

        static {
            map.put(kOk.value, kOk);
            map.put(kNotFound.value, kNotFound);
            map.put(kCorruption.value, kCorruption);
            map.put(kNotSupported.value, kNotSupported);
            map.put(kInvalidArgument.value, kInvalidArgument);
            map.put(kIOError.value, kIOError);
        }

        private Integer value;
        private String display;

        Code(Integer i, String display) {
            this.value = i;
            this.display = display;
        }

        public static Code valueOf(Integer code) {
            return map.getOrDefault(code, null);
        }
    }
}