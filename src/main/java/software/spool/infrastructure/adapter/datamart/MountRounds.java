package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.mounter.api.port.MountTarget;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The rounds of the mounts on a data mart, and the list of what each mount wrote, kept in the data mart itself under
 * {@code .spool/written/}, out of the folders of its layers, so no reader takes them for data.
 *
 * <p>A mount runs one round at a time, wherever it runs: a round takes the turn of its mount, an object that only one
 * round can create, before it starts, waits while another round holds it, and renews it while it is open. A turn that is
 * not renewed in time, because its process stopped, is taken by the next round, and only one round can take it.</p>
 *
 * <p>While a round is open, every object written for its target, or for a part of its scope, is noted as the mount's,
 * and saved every few objects. When the round ends well, what the list of the mount had, or what a round of a stopped
 * process saved, and was not written again, is removed, and the list becomes what the round wrote; nothing outside them
 * is ever touched, so mounts that write to the same partition never remove each other's objects. When the round fails,
 * nothing is removed and what it wrote is added to the list. A round that lost its turn removes nothing and keeps no
 * list, and leaves what it wrote saved for the round that holds the turn.</p>
 *
 * <p>Taking the turn of a stopped round leaves a small marker under {@code turn-taken/}, named after that round, so a
 * second round that read the same expired turn cannot take it too.</p>
 */
final class MountRounds {
    private static final String ROOT = ".spool/written/";
    private static final String LISTS = "/lists/";
    private static final String ROUNDS = "/rounds/";
    private static final String TURN = "/turn";
    private static final String TAKEN = "/turn-taken/";
    private static final String SEPARATOR = "/";
    private static final String LIST_EXTENSION = ".txt";
    private static final String LINE = "\n";
    private static final String FIELD = " ";

    private final DataMartStore store;
    private final MountRoundSettings settings;
    private final Clock clock;
    private final Map<MountTarget, OpenRound> open = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewals = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "spool-mount-round-renewal");
        thread.setDaemon(true);
        return thread;
    });

    MountRounds(DataMartStore store, MountRoundSettings settings) {
        this(store, settings, Clock.systemUTC());
    }

    MountRounds(DataMartStore store, MountRoundSettings settings, Clock clock) {
        this.store = store;
        this.settings = settings;
        this.clock = clock;
    }

    /** Waits for the turn of the mount of the target, and opens a round with it. */
    void begin(MountTarget target) {
        String owner = ownerFolder(target);
        String id = UUID.randomUUID().toString();
        takeTurn(owner, id);
        OpenRound round = new OpenRound(owner, id);
        round.renewal = renewals.scheduleAtFixedRate(round::renew, settings.renewEvery().toMillis(),
                settings.renewEvery().toMillis(), TimeUnit.MILLISECONDS);
        open.put(target, round);
    }

    /** Notes the object as written by every open round whose scope holds the target it was written for. */
    void note(MountTarget target, String path) {
        open.forEach((round, written) -> {
            if (isPartOf(target, round)) written.note(path);
        });
    }

    void complete(MountTarget target) {
        OpenRound round = open.remove(target);
        if (round == null) return;
        round.stopRenewing();
        if (!holdsTurn(round)) {
            round.saveUnsaved();
            throw new IllegalStateException("Round of " + target.qualifiedDataMart() + " for " + target.sourceKey().value()
                    + " ended after another round took its turn; nothing was removed");
        }
        Map<String, Set<String>> before = before(round);
        Map<String, Set<String>> written = round.byFolder();
        before.forEach((folder, names) -> names.stream()
                .filter(name -> !written.getOrDefault(folder, Set.of()).contains(name))
                .forEach(name -> store.delete(folder + SEPARATOR + name)));
        keepList(round.owner, written, before.keySet());
        end(round);
    }

    void abandon(MountTarget target) {
        OpenRound round = open.remove(target);
        if (round == null) return;
        round.stopRenewing();
        if (!holdsTurn(round)) {
            round.saveUnsaved();
            return;
        }
        Map<String, Set<String>> kept = before(round);
        round.byFolder().forEach((folder, names) -> kept.computeIfAbsent(folder, ignored -> new HashSet<>()).addAll(names));
        keepList(round.owner, kept, kept.keySet());
        end(round);
    }

    private void takeTurn(String owner, String id) {
        String turn = owner + TURN;
        while (true) {
            if (store.createIfAbsent(turn, turnOf(id))) return;
            Optional<Turn> held = store.read(turn).map(Turn::of);
            if (held.isPresent() && held.get().expiresAt().isBefore(clock.instant())
                    && store.createIfAbsent(owner + TAKEN + held.get().id(), id.getBytes(StandardCharsets.UTF_8))) {
                store.write(turn, turnOf(id));
                return;
            }
            if (held.isPresent()) pause();
        }
    }

    private boolean holdsTurn(OpenRound round) {
        return !round.lost && store.read(round.owner + TURN).map(Turn::of).filter(held -> held.id().equals(round.id)).isPresent();
    }

    private void end(OpenRound round) {
        store.objectsUnder(round.owner + ROUNDS.substring(0, ROUNDS.length() - SEPARATOR.length())).forEach(store::delete);
        if (holdsTurn(round)) store.delete(round.owner + TURN);
    }

    private Map<String, Set<String>> before(OpenRound current) {
        Map<String, Set<String>> before = new HashMap<>();
        String lists = current.owner + LISTS;
        for (String list : store.objectsUnder(lists.substring(0, lists.length() - SEPARATOR.length()))) {
            String folder = list.substring(lists.length(), list.length() - LIST_EXTENSION.length());
            before.computeIfAbsent(folder, ignored -> new HashSet<>()).addAll(lines(list));
        }
        String rounds = current.owner + ROUNDS;
        for (String saved : store.objectsUnder(rounds.substring(0, rounds.length() - SEPARATOR.length()))) {
            if (saved.startsWith(current.prefix)) continue;
            lines(saved).forEach(path -> before.computeIfAbsent(folderOf(path), ignored -> new HashSet<>()).add(nameOf(path)));
        }
        return before;
    }

    private void keepList(String owner, Map<String, Set<String>> byFolder, Set<String> foldersBefore) {
        String lists = owner + LISTS;
        byFolder.forEach((folder, names) -> store.write(lists + folder + LIST_EXTENSION,
                String.join(LINE, names).getBytes(StandardCharsets.UTF_8)));
        foldersBefore.stream()
                .filter(folder -> !byFolder.containsKey(folder))
                .forEach(folder -> store.delete(lists + folder + LIST_EXTENSION));
    }

    private List<String> lines(String path) {
        return store.read(path)
                .map(content -> new String(content, StandardCharsets.UTF_8))
                .map(text -> text.isEmpty() ? List.<String>of() : List.of(text.split(LINE)))
                .orElse(List.of());
    }

    private byte[] turnOf(String id) {
        return (id + FIELD + clock.instant().plus(settings.turnLasts()).toEpochMilli()).getBytes(StandardCharsets.UTF_8);
    }

    private void pause() {
        try {
            Thread.sleep(settings.waitEvery().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the turn of a mount", e);
        }
    }

    private static boolean isPartOf(MountTarget target, MountTarget round) {
        return target.qualifiedDataMart().equals(round.qualifiedDataMart())
                && target.sourceKey().contains(round.sourceKey());
    }

    private static String ownerFolder(MountTarget target) {
        String owner = target.sourceKey().value() + (target.slice() == null ? "" : SEPARATOR + target.slice());
        return ROOT + target.qualifiedDataMart() + SEPARATOR + IdempotencyKey.of(target.qualifiedDataMart(), owner).value();
    }

    private static String folderOf(String path) {
        return path.substring(0, path.lastIndexOf(SEPARATOR));
    }

    private static String nameOf(String path) {
        return path.substring(path.lastIndexOf(SEPARATOR) + SEPARATOR.length());
    }

    /** The round that holds the turn of a mount, and until when, as the turn object says. */
    private record Turn(String id, Instant expiresAt) {
        static Turn of(byte[] content) {
            String[] fields = new String(content, StandardCharsets.UTF_8).split(FIELD);
            return new Turn(fields[0], Instant.ofEpochMilli(Long.parseLong(fields[1])));
        }
    }

    /** What an open round has written, saved to the data mart every few objects, and its turn. */
    private final class OpenRound {
        private final String owner;
        private final String id;
        private final String prefix;
        private final Map<String, Set<String>> written = new HashMap<>();
        private final List<String> unsaved = new ArrayList<>();
        private ScheduledFuture<?> renewal;
        private volatile boolean lost;
        private int saves;

        private OpenRound(String owner, String id) {
            this.owner = owner;
            this.id = id;
            this.prefix = owner + ROUNDS + id + SEPARATOR;
        }

        synchronized void note(String path) {
            written.computeIfAbsent(folderOf(path), ignored -> new HashSet<>()).add(nameOf(path));
            unsaved.add(path);
            if (unsaved.size() >= settings.saveEvery()) saveUnsaved();
        }

        synchronized void saveUnsaved() {
            if (unsaved.isEmpty()) return;
            store.write(prefix + saves++ + LIST_EXTENSION, String.join(LINE, unsaved).getBytes(StandardCharsets.UTF_8));
            unsaved.clear();
        }

        synchronized Map<String, Set<String>> byFolder() {
            return written;
        }

        void renew() {
            if (lost) return;
            try {
                Optional<Turn> held = store.read(owner + TURN).map(Turn::of);
                if (held.isEmpty() || !held.get().id().equals(id)) {
                    lost = true;
                    return;
                }
                store.write(owner + TURN, turnOf(id));
            } catch (RuntimeException e) {
                return;
            }
        }

        void stopRenewing() {
            renewal.cancel(false);
        }
    }
}
