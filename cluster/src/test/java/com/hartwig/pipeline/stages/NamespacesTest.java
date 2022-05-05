package com.hartwig.pipeline.stages;

import static java.util.stream.Collectors.toList;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;

public class NamespacesTest {
    @Test
    public void everyStageIsAnnotatedWithNonEmptyNamespace() {
        allConcreteStages().stream().forEach(subType -> {
            Namespace namespace = subType.getAnnotation(Namespace.class);
            assertThat(namespace).as("%s is annotated with a namespace", subType.getSimpleName()).isNotNull();
            assertThat(namespace.value()).as("Namespace for %s is null or empty", subType.getSimpleName()).isNotEmpty();
        });
    }

    @Test
    public void everyStageNamespaceIsUnique() {
        List<String> namespaces = new ArrayList<>(allNamespaces().stream().map(n -> n.value()).collect(toList()));
        assertThat(namespaces.stream().sorted().collect(toList())).isEqualTo(namespaces.stream().distinct().sorted().collect(toList()));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static List<Class<Stage>> allConcreteStages() {
        Reflections scanner = new Reflections(ClasspathHelper.forPackage("com.hartwig.pipeline"), new SubTypesScanner(false));
        List<Class<Stage>> stages = new ArrayList<>();
        scanner.getSubTypesOf(Object.class).stream().filter(t -> t != null).forEach(subType -> {
            if (implementsStage(subType) && !Modifier.isAbstract(subType.getModifiers())) {
                stages.add((Class<Stage>) subType);
            }
        });
        return stages;
    }

    private static boolean implementsStage(final Class<?> cut) {
        if (cut == null || cut == Object.class) {
            return false;
        }
        if (cut.getInterfaces() != null && Arrays.stream(cut.getInterfaces()).anyMatch(i -> i == Stage.class)) {
            return true;
        }
        return implementsStage(cut.getSuperclass());
    }

    public static List<Namespace> allNamespaces() {
        return allConcreteStages().stream().map(stage -> stage.getAnnotation(Namespace.class)).collect(toList());
    }
}
