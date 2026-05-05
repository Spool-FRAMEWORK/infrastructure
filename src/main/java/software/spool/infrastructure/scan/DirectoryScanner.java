package software.spool.infrastructure.scan;

import software.spool.infrastructure.spi.Plugin;
import java.io.File;

public final class DirectoryScanner {
    public <T extends Plugin<R>, R> void scan(File root, Class<T> type, ClassLoader cl) {
        scanRecursive(root, root, type, cl);
    }

    private <T extends Plugin<R>, R> void scanRecursive(File root, File current, Class<T> type, ClassLoader cl) {
        File[] files = current.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.isDirectory()) {
                scanRecursive(root, f, type, cl);
            } else if (f.getName().endsWith(".class")) {
                String relative = root.toURI().relativize(f.toURI()).getPath();
                String className = relative.replace('/', '.').replace('\\', '.').replaceAll("\\.class$", "");
                PluginCandidateLoader.tryRegister(className, type, cl);
            }
        }
    }
}