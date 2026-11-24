package software.spool.infrastructure.adapter.datamart;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.mounter.api.port.MountTarget;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
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
 * <p>A mount runs one round at a time, wherever it runs. A round holds the turn of its mount, a numbered object that
 * only one round can create, and renews it while it is open by changing its content. A round that waits takes the turn,
 * by creating the next number, only after it has seen the same content unchanged for as long as the turn says it
 * lasts, measured by its own process, so the clocks of different processes are never compared. Ending a round removes its turn and any
 * older one, so nothing of the turns is left behind.</p>
 *
 * <p>While a round is open, every object written for its target, or for a part of its scope, is noted as the mount's,
 * and saved by partition every few objects. When the round ends well, what the list of the mount had, or what a round
 * of a stopped process saved, and was not written again, is removed, and the list becomes what the round wrote. It is
 * done one partition at a time, so a round never holds in memory more than one partition and the few objects it has
 * not saved yet. Nothing outside the lists is ever touched, so mounts that write to the same partition never remove
 * each other's objects. When the round fails, nothing is removed and what it wrote is added to the list. A round that
 * lost its turn removes nothing and keeps no list, and leaves what it wrote saved for the round that holds the
 * turn.</p>
 */
final class MountRounds {
    private static final String ROOT = ".spool/written/";
    private static final String LISTS = "/lists";
    private static final String ROUNDS = "/rounds";
    private static final String TURN = "/turn";
    private static final String SEPARATOR = "/";
    private static final String EXTENSION = ".txt";
    private static final String LINE = "\n";
    private static final String FIELD = " ";

    private final DataMartStore store;
    private final MountRoundSettings settings;
    private final Map<MountTarget, OpenRound> open = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewals = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "spool-mount-round-renewal");
        thread.setDaemon(true);
        return thread;
    });

    MountRounds(DataMartStore store, MountRoundSettings settings) {
        this.store = store;
        this.settings = settings;
    }

    /** Waits for the turn of the mount of the target, and opens a round with it. */
    void begin(MountTarget target) {
        String owner = ownerFolder(target);
        String id = UUID.randomUUID().toString();
        long turn = takeTurn(owner, id);
        OpenRound round = new OpenRound(owner, id, turn);
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
        round.saveUnsaved();
        if (!holdsTurn(round)) {
            throw new IllegalStateException("Round of " + target.qualifiedDataMart() + " for " + target.sourceKey().value()
                    + " ended after another round took its turn; nothing was removed");
        }
        Saved saved = savedOf(round);
        for (String folder : saved.folders()) {
            Set<String> written = namesIn(saved.ownParts(folder));
            Set<String> before = namesIn(saved.list(folder));
            before.addAll(namesIn(saved.otherParts(folder)));
            before.stream().filter(name -> !written.contains(name)).forEach(name -> store.delete(folder + SEPARATOR + name));
            keepList(round.owner, folder, written);
        }
        end(round);
    }

    void abandon(MountTarget target) {
        OpenRound round = open.remove(target);
        if (round == null) return;
        round.stopRenewing();
        round.saveUnsaved();
        if (!holdsTurn(round)) return;
        Saved saved = savedOf(round);
        for (String folder : saved.folders()) {
            Set<String> kept = namesIn(saved.list(folder));
            kept.addAll(namesIn(saved.ownParts(folder)));
            kept.addAll(namesIn(saved.otherParts(folder)));
            keepList(round.owner, folder, kept);
        }
        end(round);
    }

    private long takeTurn(String owner, String id) {
        Optional<Sighting> sighting = Optional.empty();
        while (true) {
            TreeMap<Long, String> turns = turnsOf(owner);
            if (turns.isEmpty()) {
                if (store.createIfAbsent(turnPath(owner, 0), turnContent(id, 0))) return 0;
                continue;
            }
            long latest = turns.lastKey();
            Optional<String> content = store.read(turns.get(latest)).map(MountRounds::text);
            if (content.isEmpty()) continue;
            if (sighting.filter(seen -> seen.isOf(latest, content.get())).isEmpty()) {
                sighting = Optional.of(new Sighting(latest, content.get(), settings.elapsed().getAsLong()));
            } else if (sighting.get().unchangedFor(settings.elapsed().getAsLong()).compareTo(sighting.get().lasts()) >= 0
                    && store.createIfAbsent(turnPath(owner, latest + 1), turnContent(id, 0))) {
                removeTurnsBelow(owner, latest + 1);
                return latest + 1;
            }
            pause();
        }
    }

    private boolean holdsTurn(OpenRound round) {
        if (round.lost) return false;
        TreeMap<Long, String> turns = turnsOf(round.owner);
        return !turns.isEmpty() && turns.lastKey() == round.turn
                && store.read(turns.get(round.turn)).map(MountRounds::text)
                .filter(content -> content.startsWith(round.id + FIELD)).isPresent();
    }

    private void end(OpenRound round) {
        store.objectsUnder(round.owner + ROUNDS).forEach(store::delete);
        if (holdsTurn(round)) removeTurnsBelow(round.owner, round.turn + 1);
    }

    private void removeTurnsBelow(String owner, long turn) {
        turnsOf(owner).headMap(turn).values().forEach(store::delete);
    }

    private TreeMap<Long, String> turnsOf(String owner) {
        TreeMap<Long, String> turns = new TreeMap<>();
        for (String path : store.objectsIn(owner + TURN)) {
            numberOf(nameOf(path)).ifPresent(number -> turns.put(number, path));
        }
        return turns;
    }

    /** Where the lists of the mount and the parts saved by its rounds are, by the partition they are about. */
    private Saved savedOf(OpenRound round) {
        Saved saved = new Saved();
        String lists = round.owner + LISTS + SEPARATOR;
        for (String list : store.objectsUnder(round.owner + LISTS)) {
            saved.lists.put(list.substring(lists.length(), list.length() - EXTENSION.length()), list);
        }
        String rounds = round.owner + ROUNDS + SEPARATOR;
        for (String part : store.objectsUnder(round.owner + ROUNDS)) {
            String inRounds = part.substring(rounds.length());
            String id = inRounds.substring(0, inRounds.indexOf(SEPARATOR));
            String folder = folderOf(inRounds.substring(id.length() + SEPARATOR.length()));
            (id.equals(round.id) ? saved.own : saved.others).computeIfAbsent(folder, ignored -> new ArrayList<>()).add(part);
        }
        return saved;
    }

    private Set<String> namesIn(List<String> paths) {
        Set<String> names = new HashSet<>();
        paths.forEach(path -> names.addAll(lines(path)));
        return names;
    }

    private void keepList(String owner, String folder, Set<String> names) {
        String list = owner + LISTS + SEPARATOR + folder + EXTENSION;
        if (names.isEmpty()) store.delete(list);
        else store.write(list, String.join(LINE, names).getBytes(StandardCharsets.UTF_8));
    }

    private List<String> lines(String path) {
        return store.read(path)
                .map(MountRounds::text)
                .map(text -> text.isEmpty() ? List.<String>of() : List.of(text.split(LINE)))
                .orElse(List.of());
    }

    private void pause() {
        try {
            Thread.sleep(settings.waitEvery().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the turn of a mount", e);
        }
    }

    private static String text(byte[] content) {
        return new String(content, StandardCharsets.UTF_8);
    }

    /** Who holds the turn, how many times it renewed it, and how long it lasts without being renewed. */
    private byte[] turnContent(String id, long renewals) {
        return String.join(FIELD, id, String.valueOf(renewals), String.valueOf(settings.turnLasts().toMillis()))
                .getBytes(StandardCharsets.UTF_8);
    }

    private static String turnPath(String owner, long number) {
        return owner + TURN + SEPARATOR + number + EXTENSION;
    }

    private static OptionalLong numberOf(String name) {
        if (!name.endsWith(EXTENSION)) return OptionalLong.empty();
        try {
            return OptionalLong.of(Long.parseLong(name.substring(0, name.length() - EXTENSION.length())));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
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

    /** A turn as a waiting round saw it, and when, by the count of its own process, it first saw it so. */
    private record Sighting(long turn, String content, long seenAt) {
        boolean isOf(long otherTurn, String otherContent) {
            return turn == otherTurn && content.equals(otherContent);
        }

        Duration unchangedFor(long now) {
            return Duration.ofNanos(now - seenAt);
        }

        /** How long the round that holds the turn says it lasts without being renewed. */
        Duration lasts() {
            return Duration.ofMillis(Long.parseLong(content.split(FIELD)[2]));
        }
    }

    /** The paths of the lists of a mount and of the parts its rounds saved, by partition folder. */
    private static final class Saved {
        private final Map<String, String> lists = new HashMap<>();
        private final Map<String, List<String>> own = new HashMap<>();
        private final Map<String, List<String>> others = new HashMap<>();

        Set<String> folders() {
            Set<String> folders = new HashSet<>(lists.keySet());
            folders.addAll(own.keySet());
            folders.addAll(others.keySet());
            return folders;
        }

        List<String> list(String folder) {
            return Optional.ofNullable(lists.get(folder)).map(List::of).orElse(List.of());
        }

        List<String> ownParts(String folder) {
            return own.getOrDefault(folder, List.of());
        }

        List<String> otherParts(String folder) {
            return others.getOrDefault(folder, List.of());
        }
    }

    /** What an open round has written and not saved yet, by partition folder, and its turn. */
    private final class OpenRound {
        private final String owner;
        private final String id;
        private final long turn;
        private final Map<String, List<String>> unsaved = new HashMap<>();
        private int unsavedCount;
        private int saves;
        private long renewals;
        private ScheduledFuture<?> renewal;
        private volatile boolean lost;

        private OpenRound(String owner, String id, long turn) {
            this.owner = owner;
            this.id = id;
            this.turn = turn;
        }

        synchronized void note(String path) {
            unsaved.computeIfAbsent(folderOf(path), ignored -> new ArrayList<>()).add(nameOf(path));
            if (++unsavedCount >= settings.saveEvery()) saveUnsaved();
        }

        synchronized void saveUnsaved() {
            int save = saves++;
            unsaved.forEach((folder, names) -> store.write(owner + ROUNDS + SEPARATOR + id + SEPARATOR + folder
                    + SEPARATOR + save + EXTENSION, String.join(LINE, names).getBytes(StandardCharsets.UTF_8)));
            unsaved.clear();
            unsavedCount = 0;
        }

        synchronized void renew() {
            if (lost) return;
            try {
                if (!holdsTurn(this)) {
                    lost = true;
                    return;
                }
                store.write(turnPath(owner, turn), turnContent(id, ++renewals));
            } catch (RuntimeException e) {
                return;
            }
        }

        void stopRenewing() {
            renewal.cancel(false);
        }
    }
}
