package software.spool.infrastructure.scan;

import software.spool.infrastructure.spi.Plugin;
import java.io.File;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class JarScanner {

    public <T extends Plugin<R>, R> void scan(File jarFile, Class<T> type, ClassLoader cl) {
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (isLoadableClass(name)) {
                    String className = name.replace('/', '.').replaceAll("\\.class$", "");
                    PluginCandidateLoader.tryRegister(className, type, cl);
                }
            }
        } catch (Exception ignored) {}
    }

    private boolean isLoadableClass(String entryName) {
        if (!entryName.endsWith(".class")) return false;
        if (entryName.contains("$")) return false;
        if (entryName.startsWith("META-INF/")) return false;
        if (entryName.equals("module-info.class")) return false;
        return !entryName.contains("/module-info.class");
    }
}