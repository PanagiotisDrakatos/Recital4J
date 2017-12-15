package com.github.Builder;

import org.apache.commons.io.IOUtils;

import java.io.*;

public class Policies {

    private static final String fileName = "winutils.exe";
    private static final String directoryName = "bin";


    protected static void Path_Setup() throws IOException {
        final InputStream in = Policies.class.getResourceAsStream("/" + fileName);
        final File directory = new File(directoryName);
        final boolean val = !directory.exists() ? directory.mkdir() : true;

        File dest = new File(directoryName + "/" + fileName);
        System.out.println(dest.getAbsolutePath());
        OutputStream os = new FileOutputStream(dest);
        IOUtils.copy(in, os);


        System.out.println("Setup was Established Correct at path: " + directory.getAbsoluteFile().getParentFile().getAbsolutePath());
        System.setProperty("hadoop.home.dir", directory.getAbsoluteFile().getParentFile().getAbsolutePath());
    }


}
