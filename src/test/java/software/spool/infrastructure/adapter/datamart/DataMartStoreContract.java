package software.spool.infrastructure.adapter.datamart;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** What every store of a data mart does with its objects, the creation that only one caller can win included. */
abstract class DataMartStoreContract {
    abstract DataMartStore store();

    @Test
    void createIfAbsent_pathWithoutAnObject_createsItAndSaysSo() {
        assertThat(store().createIfAbsent(".spool/written/lock", bytes("first"))).isTrue();

        assertThat(store().read(".spool/written/lock")).hasValueSatisfying(content -> assertThat(content).isEqualTo(bytes("first")));
    }

    @Test
    void createIfAbsent_pathWithAnObject_leavesItAsItWasAndSaysSo() {
        store().createIfAbsent(".spool/written/lock", bytes("first"));

        assertThat(store().createIfAbsent(".spool/written/lock", bytes("second"))).isFalse();
        assertThat(store().read(".spool/written/lock")).hasValueSatisfying(content -> assertThat(content).isEqualTo(bytes("first")));
    }

    @Test
    void createIfAbsent_afterTheObjectIsRemoved_createsItAgain() {
        store().createIfAbsent(".spool/written/lock", bytes("first"));
        store().delete(".spool/written/lock");

        assertThat(store().createIfAbsent(".spool/written/lock", bytes("second"))).isTrue();
    }

    @Test
    void objectsIn_folder_listsOnlyTheObjectsDirectlyInside() {
        store().write("gold/totals/country=ES/a.json", bytes("a"));
        store().write("gold/totals/country=ES/day=1/b.json", bytes("b"));

        assertThat(store().objectsIn("gold/totals/country=ES")).containsExactly("gold/totals/country=ES/a.json");
    }

    @Test
    void read_pathWithoutAnObject_isEmpty() {
        assertThat(store().read("gold/totals/missing.json")).isEmpty();
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
