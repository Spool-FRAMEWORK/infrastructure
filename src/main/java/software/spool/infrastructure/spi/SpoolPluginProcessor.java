package software.spool.infrastructure.spi;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

@SupportedAnnotationTypes("software.spool.infrastructure.spi.SpoolPlugin")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class SpoolPluginProcessor extends AbstractProcessor {

    private final Map<String, Set<String>> serviceEntries = new LinkedHashMap<>();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(SpoolPlugin.class)) {
            SpoolPlugin annotation = element.getAnnotation(SpoolPlugin.class);
            String implementationClass = ((TypeElement) element).getQualifiedName().toString();

            String serviceInterface;
            try {
                serviceInterface = annotation.value().getName();
            } catch (Exception e) {
                serviceInterface = extractMirrorValue(element);
            }

            if (serviceInterface == null) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "@SpoolPlugin: could not resolve service interface",
                        element
                );
                continue;
            }

            serviceEntries
                    .computeIfAbsent(serviceInterface, k -> new LinkedHashSet<>())
                    .add(implementationClass);
        }

        if (roundEnv.processingOver()) {
            writeServiceFiles();
        }

        return true;
    }

    private String extractMirrorValue(Element element) {
        return element.getAnnotationMirrors().stream()
                .filter(m -> m.getAnnotationType().toString()
                        .equals(SpoolPlugin.class.getName()))
                .flatMap(m -> m.getElementValues().entrySet().stream())
                .filter(e -> e.getKey().getSimpleName().toString().equals("value"))
                .map(e -> e.getValue().getValue().toString())
                .findFirst()
                .orElse(null);
    }

    private void writeServiceFiles() {
        for (Map.Entry<String, Set<String>> entry : serviceEntries.entrySet()) {
            try {
                FileObject file = processingEnv.getFiler().createResource(
                        StandardLocation.CLASS_OUTPUT,
                        "",
                        "META-INF/services/" + entry.getKey()
                );
                try (PrintWriter writer = new PrintWriter(file.openWriter())) {
                    entry.getValue().forEach(writer::println);
                }
            } catch (IOException e) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Failed to write META-INF/services for " + entry.getKey() + ": " + e.getMessage()
                );
            }
        }
    }
}