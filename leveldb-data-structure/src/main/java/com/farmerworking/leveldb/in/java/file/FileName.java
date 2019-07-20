package com.farmerworking.leveldb.in.java.file;

import com.farmerworking.leveldb.in.java.api.Status;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;

import static com.farmerworking.leveldb.in.java.file.FileType.*;

public class FileName {
    public static String logFileName(String dbName, long number) {
        assert(number > 0);
        return makeFileName(dbName, number, "log");
    }

    public static String tableFileName(String dbName, long number) {
        assert(number > 0);
        return makeFileName(dbName, number, "ldb");
    }

    public static String SSTTableFileName(String dbName, long number) {
        assert(number > 0);
        return makeFileName(dbName, number, "sst");
    }

    public static String descriptorFileName(String dbName, long number) {
        assert(number > 0);
        return dbName + String.format("/MANIFEST-%s", Long.toUnsignedString(number));
    }

    public static String currentFileName(String dbName) {
        return dbName + "/CURRENT";
    }

    public static String lockFileName(String dbName) {
        return dbName + "/LOCK";
    }

    public static String tempFileName(String dbName, long number) {
        assert(number > 0);
        return makeFileName(dbName, number, "dbtmp");
    }

    public static String infoLogFileName(String dbName) {
        return dbName + "/LOG";
    }

    public static String oldInfoLogFileName(String dbName) {
        return dbName + "/LOG.old";
    }

    public static Pair<Long, FileType> parseFileName(String fileName) {
        if (fileName.equals("CURRENT")) {
            return new Pair<>(0L, FileType.kCurrentFile);
        } else if (fileName.equals("LOCK")) {
            return new Pair<>(0L, FileType.kDBLockFile);
        } else if (fileName.equals("LOG") || fileName.equals("LOG.old")) {
            return new Pair<>(0L, FileType.kInfoLogFile);
        } else if (fileName.startsWith("MANIFEST-")) {
            String tail = fileName.substring("MANIFEST-".length());
            if (tail.length() > 6) {
                return null;
            } else if (StringUtils.isNumeric(tail)) {
                return new Pair<>(Long.parseUnsignedLong(tail), FileType.kDescriptorFile);
            } else {
                return null;
            }
        } else {
            int index = fileName.indexOf(".");
            if (index == -1) {
                return null;
            } else {
                String middle = fileName.substring(0, index);
                String tail = fileName.substring(index);

                try {
                    if (!StringUtils.isNumeric(middle)) {
                        return null;
                    } else if (tail.equals(".log")) {
                        return new Pair<>(Long.parseUnsignedLong(middle), kLogFile);
                    } else if (tail.equals(".sst") || tail.equals(".ldb")) {
                        return new Pair<>(Long.parseUnsignedLong(middle), kTableFile);
                    } else if (tail.equals(".dbtmp")) {
                        return new Pair<>(Long.parseUnsignedLong(middle), kTempFile);
                    } else {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
    }

    public static Status setCurrentFile(Env env, String dbname, long descriptorNumber) {
        // Remove leading "dbname/" and add newline to manifest file name
        String manifest = descriptorFileName(dbname, descriptorNumber);
        assert manifest.startsWith(dbname + "/");
        String content = manifest.substring(dbname.length() + 1);
        String tmp = tempFileName(dbname, descriptorNumber);
        Status status = Env.writeStringToFileSync(env, content + "\n", tmp);

        if (status.isOk()) {
            status = env.renameFile(tmp, currentFileName(dbname));
        }

        if (status.isNotOk()) {
            env.delete(tmp);
        }

        return status;
    }

    private static String makeFileName(String dbName, long number, String suffix) {
        return dbName + String.format("/%s.%s", Long.toUnsignedString(number), suffix);
    }
}
