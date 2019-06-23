package com.farmerworking.leveldb.in.java.data.structure.version;

import lombok.Data;

@Data
public class GetStats {
    private FileMetaData seekFile;
    private int seekFileLevel;
}
