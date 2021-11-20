package site.ycsb.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ZiplogClientJNI {

    static {
        int length;
        byte[] buffer = new byte[1024];
        InputStream is = ZiplogClientJNI.class.getResourceAsStream("/lib/libziplogKvsClient.so");
        if (is == null) {
            throw new RuntimeException("lib not exist!\n");
        }
        try {
            File file = File.createTempFile("libziplog", ".so");
            OutputStream os = new FileOutputStream(file);
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
            System.load(file.getAbsolutePath());
            is.close();
            os.close();
        file.deleteOnExit();
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException();
        } 
    }

    public native String get(String key);

    public native boolean put(String key, String value);
}