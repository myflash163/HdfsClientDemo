package com.atguigu;

import java.io.File;

public class Util {
    public static void delete(String path){
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (File file0 : files) {
                file0.delete();
            }
            file.delete();
        }
    }
}
