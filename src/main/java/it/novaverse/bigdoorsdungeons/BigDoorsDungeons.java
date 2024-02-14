package it.novaverse.bigdoorsdungeons;

import net.playavalon.mythicdungeons.MythicDungeons;
import net.playavalon.mythicdungeons.dungeons.Dungeon;
import net.playavalon.mythicdungeons.dungeons.Instance;
import nl.pim16aap2.bigDoors.BigDoors;
import nl.pim16aap2.bigDoors.Door;
import nl.pim16aap2.bigDoors.storage.sqlite.SQLiteJDBCDriverConnection;
import nl.pim16aap2.bigDoors.util.DoorDirection;
import nl.pim16aap2.bigDoors.util.DoorType;
import nl.pim16aap2.bigDoors.util.RotateDirection;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.world.WorldLoadEvent;
import org.bukkit.event.world.WorldUnloadEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.Blocking;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class BigDoorsDungeons extends JavaPlugin implements Listener {

    private Map<String, Long> dungeonDoorNameToId;

    public Long getDungeonDoorId(String name) {
        return dungeonDoorNameToId.get(name);
    }

    @Override
    public void onEnable() {
        dungeonDoorNameToId = new ConcurrentHashMap<>();
        getServer().getPluginManager().registerEvents(this, this);

        if (getServer().getPluginManager().isPluginEnabled("PlaceholderAPI")) {
            new BigDoorsDungeonsPlaceholders(this).register();
        }
    }

    @Override
    public void onDisable() {
        getSLF4JLogger().info("Plugin is being disabled... removing doors from dungeon worlds!");
        var bigDoorsStorage = getBigDoorsStorage();
        var bigDoorsCache = BigDoors.get().getPBCache();
        MythicDungeons.inst().getDungeons().getAll().stream()
                .map(Dungeon::getInstances)
                .flatMap(Collection::stream)
                .forEach(instance -> {
                    getSLF4JLogger().info("> Removing doors from dungeon world " + instance.getInstanceWorld().getName());
                    bigDoorsStorage.removeDoorsFromWorld(instance.getInstanceWorld())
                            .forEach(door -> {
                                bigDoorsCache.invalidate(door.getDoorUID());
                            });
                });

        dungeonDoorNameToId = null;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onWorldLoad(WorldLoadEvent event) {
        var world = event.getWorld();

        getServer().getScheduler().runTask(this, () -> {
            var instance = getDungeonInstance(world);
            if (instance == null) {
                return;
            }
            var dungeon = instance.getDungeon();

            var bigDoorsStorage = getBigDoorsStorage();
            var bigDoorsCache = BigDoors.get().getPBCache();
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                // Remove existing doors
                bigDoorsStorage.removeDoorsFromWorld(world)
                        .forEach(door -> {
                            bigDoorsCache.invalidate(door.getDoorUID());
                            dungeonDoorNameToId.remove(door.getName());
                        });

                var dungeonUuid = UUID.nameUUIDFromBytes(dungeon.getWorldName().getBytes(StandardCharsets.UTF_8));
                var doors = getDoorsCopyForDungeon(dungeonUuid, world, !instance.isEditMode()); // Add suffix if not in edit mode, prevent name conflicts
                doors.forEach(door -> {
                    var id = bigDoorsStorage.insert(door);
                    dungeonDoorNameToId.put(door.getName(), id);
                });
                getSLF4JLogger().info("Loaded dungeon " + world.getName() + " doors! (" + doors.size() + ")");
            });
        });
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onWorldUnload(WorldUnloadEvent event) {
        var world = event.getWorld();

        var instance = getDungeonInstance(world);
        if (instance == null) {
            return;
        }
        var dungeon = instance.getDungeon();
        var editMode = instance.isEditMode();

        var bigDoorsStorage = getBigDoorsStorage();
        var bigDoorsCache = BigDoors.get().getPBCache();
        getServer().getScheduler().runTaskAsynchronously(this, () -> {
            if (editMode) {
                var doors = bigDoorsStorage.getDoorsInWorld(world);
                doors.forEach(door -> {
                    bigDoorsCache.invalidate(door.getDoorUID());
                    dungeonDoorNameToId.remove(door.getName());
                });
                getSLF4JLogger().info("Saving dungeon " + world.getName() + " doors! (" + doors.size() + ")");

                var dungeonUuid = UUID.nameUUIDFromBytes(dungeon.getWorldName().getBytes(StandardCharsets.UTF_8));
                deleteDoorsInWorldRaw(dungeonUuid);
                changeDoorsWorld(world.getUID(), dungeonUuid);
                return;
            }

            getSLF4JLogger().info("Removing doors from dungeon world " + world.getName());
            bigDoorsStorage.removeDoorsFromWorld(world)
                    .forEach(door -> {
                        bigDoorsCache.invalidate(door.getDoorUID());
                        dungeonDoorNameToId.remove(door.getName());
                    });
        });
    }

    @Blocking
    private static void deleteDoorsInWorldRaw(UUID target) {
        var bigDoorsStorage = getBigDoorsStorage();
        try (var connection = getBigDoorsConnection(bigDoorsStorage)) {
            try (var preparedStatement = connection.prepareStatement("DELETE FROM doors WHERE world = ?")) {
                preparedStatement.setString(1, target.toString());
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Blocking
    private static void changeDoorsWorld(UUID source, UUID target) {
        var bigDoorsStorage = getBigDoorsStorage();
        try (var connection = getBigDoorsConnection(bigDoorsStorage)) {
            try (var preparedStatement = connection.prepareStatement("UPDATE doors SET world = ? WHERE world = ?")) {
                preparedStatement.setString(1, target.toString());
                preparedStatement.setString(2, source.toString());
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Blocking
    private static List<Door> getDoorsCopyForDungeon(UUID sourceWorldUuid, World targetWorld, boolean addWorldSuffix) {
        var doors = new ArrayList<Door>();

        var bigDoorsStorage = getBigDoorsStorage();
        try (var connection = getBigDoorsConnection(bigDoorsStorage)) {
            try (var preparedStatement = connection.prepareStatement("SELECT * FROM doors WHERE world = ?;")) {
                preparedStatement.setString(1, sourceWorldUuid.toString());
                try (var resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        long doorUID = resultSet.getLong(1);
                        var doorOwner = bigDoorsStorage.getOwnerOfDoor(doorUID);
                        if (doorOwner == null) {
                            BigDoors.get().getMyLogger().logMessageToConsole("Failed to obtain doorOwner of door: " + doorUID
                                    + ". This door cannot be constructed!");
                            continue;
                        }

                        var min = new Location(targetWorld, resultSet.getInt(5), resultSet.getInt(6), resultSet.getInt(7));
                        var max = new Location(targetWorld, resultSet.getInt(8), resultSet.getInt(9), resultSet.getInt(10));
                        var engine = new Location(targetWorld, resultSet.getInt(11), resultSet.getInt(12), resultSet.getInt(13));
                        var powerBlock = new Location(targetWorld, resultSet.getInt(17), resultSet.getInt(18), resultSet.getInt(19));

                        var name = resultSet.getString(2);
                        if (addWorldSuffix) {
                            name = name + "_" + targetWorld.getName();
                        }

                        var door = new Door(
                                doorOwner.getPlayerUUID(),
                                doorOwner.getPlayerName(),
                                doorOwner.getPlayerUUID(),
                                targetWorld,
                                min,
                                max,
                                engine,
                                name, // we need suffix to make it unique
                                (resultSet.getInt(4) == 1),
                                doorUID,
                                (resultSet.getInt(14) == 1),
                                doorOwner.getPermission(),
                                DoorType.valueOf(resultSet.getInt(15)),
                                DoorDirection.valueOf(resultSet.getInt(16)),
                                powerBlock,
                                RotateDirection.valueOf(resultSet.getInt(20)),
                                resultSet.getInt(21),
                                resultSet.getBoolean(24),
                                resultSet.getBoolean(25)
                        );
                        door.setBlocksToMove(resultSet.getInt(23));
                        doors.add(door);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return doors;
    }

    private static SQLiteJDBCDriverConnection getBigDoorsStorage() {
        try {
            return (SQLiteJDBCDriverConnection) FieldUtils.readDeclaredField(BigDoors.get(), "db", true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection getBigDoorsConnection(SQLiteJDBCDriverConnection storage) {
        try {
            return (Connection) MethodUtils.invokeMethod(storage, true, "getConnection");
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static Instance getDungeonInstance(World world) {
        return MythicDungeons.inst().getDungeons().getAll().stream()
                .map(Dungeon::getInstances)
                .flatMap(Collection::stream)
                .filter(current -> world.equals(current.getInstanceWorld()))
                .findAny()
                .orElse(null);
    }
}
