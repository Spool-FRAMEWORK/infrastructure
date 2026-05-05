package software.spool.infrastructure.scan;

import software.spool.infrastructure.spi.Plugin;

import java.io.File;

public final class ClasspathPluginScanner {
    private final DirectoryScanner directoryScanner = new DirectoryScanner();
    private final JarScanner jarScanner = new JarScanner();

    public <T extends Plugin<R>, R> void scan(Class<T> type) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String[] entries = System.getProperty("java.class.path", "").split(File.pathSeparator);
        for (String entry : entries) {
            File file = new File(entry);
            if (file.isDirectory()) {
                directoryScanner.scan(file, type, cl);
            } else if (entry.endsWith(".jar")) {
                jarScanner.scan(file, type, cl);
            }
        }
    }
}