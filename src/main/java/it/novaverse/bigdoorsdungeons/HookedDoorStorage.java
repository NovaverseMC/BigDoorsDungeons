package it.novaverse.bigdoorsdungeons;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import nl.pim16aap2.bigDoors.BigDoors;
import nl.pim16aap2.bigDoors.Door;
import nl.pim16aap2.bigDoors.storage.sqlite.SQLiteJDBCDriverConnection;
import nl.pim16aap2.bigDoors.util.*;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class HookedDoorStorage extends SQLiteJDBCDriverConnection {

    private final static UUID VIRTUAL_DOOR_OWNER_UUID = UUID.fromString("a8190a78-cf83-11ee-a506-0242ac120002");
    private final static String VIRTUAL_DOOR_OWNER_NAME = "BigDoorsDungeons";

    private static final int DOOR_ID = 1;
    private static final int DOOR_NAME = 2;
    private static final int DOOR_WORLD = 3;
    private static final int DOOR_OPEN = 4;
    private static final int DOOR_MIN_X = 5;
    private static final int DOOR_MIN_Y = 6;
    private static final int DOOR_MIN_Z = 7;
    private static final int DOOR_MAX_X = 8;
    private static final int DOOR_MAX_Y = 9;
    private static final int DOOR_MAX_Z = 10;
    private static final int DOOR_ENG_X = 11;
    private static final int DOOR_ENG_Y = 12;
    private static final int DOOR_ENG_Z = 13;
    private static final int DOOR_LOCKED = 14;
    private static final int DOOR_TYPE = 15;
    private static final int DOOR_ENG_SIDE = 16;
    private static final int DOOR_POWER_X = 17;
    private static final int DOOR_POWER_Y = 18;
    private static final int DOOR_POWER_Z = 19;
    private static final int DOOR_OPEN_DIR = 20;
    private static final int DOOR_AUTO_CLOSE = 21;
    private static final int DOOR_CHUNK_HASH = 22;
    private static final int DOOR_BLOCKS_TO_MOVE = 23;
    private static final int DOOR_NOTIFY = 24;
    private static final int DOOR_BYPASS_PROTECTIONS = 25;

    private final Map<Long, Door> virtualDoors;
    private final Map<String, Door> virtualDoorsByName;
    private final AtomicLong lastVirtualDoorId;

    private static UUID getVirtualWorldUUID(String worldName) {
        return UUID.nameUUIDFromBytes(worldName.getBytes(StandardCharsets.UTF_8));
    }

    public HookedDoorStorage(BigDoors bigDoors, String dbName) {
        super(bigDoors, dbName);
        virtualDoors = Collections.synchronizedMap(new Long2ObjectOpenHashMap<>());
        virtualDoorsByName = new ConcurrentHashMap<>();
        lastVirtualDoorId = new AtomicLong(-1);
    }

    public Door getVirtualDoorByName(String name) {
        return virtualDoorsByName.get(name);
    }

    // Raw SQL methods

    private Connection getSQLConnection() {
        try {
            return (Connection) MethodUtils.invokeMethod(this, true, "getConnection");
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteInWorldRaw(UUID worldUID) {
        try (var connection = getSQLConnection()) {
            try (var statement = connection.prepareStatement("DELETE FROM doors WHERE world = ?")) {
                statement.setString(1, worldUID.toString());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings({"UnusedReturnValue", "unused", "SameParameterValue"})
    private long insertRaw(UUID player, String playerName, UUID primeOwner, UUID worldUID, Location min, Location max, Location engine, String name, boolean isOpen, Long doorUID, boolean isLocked, int permission, DoorType type, DoorDirection engineSide, Location powerBlock, RotateDirection openDir, int autoClose, boolean notify, boolean bypassProtections, int blocksToMove) {
        try (var connection = getSQLConnection()) {
            var insertSql = "INSERT INTO doors(id,name,world,isOpen,xMin,yMin,zMin,xMax,yMax,zMax,engineX,engineY,engineZ,isLocked,type,engineSide,powerBlockX,powerBlockY,powerBlockZ,openDirection,autoClose,chunkHash,blocksToMove,notify,bypass_protections) "
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            try (var statement = connection.prepareStatement(insertSql)) {
                statement.setLong(DOOR_ID, doorUID);
                statement.setString(DOOR_NAME, name);
                statement.setString(DOOR_WORLD, worldUID.toString());
                statement.setInt(DOOR_OPEN, isOpen ? 1 : 0);
                statement.setInt(DOOR_MIN_X, min.getBlockX());
                statement.setInt(DOOR_MIN_Y, min.getBlockY());
                statement.setInt(DOOR_MIN_Z, min.getBlockZ());
                statement.setInt(DOOR_MAX_X, max.getBlockX());
                statement.setInt(DOOR_MAX_Y, max.getBlockY());
                statement.setInt(DOOR_MAX_Z, max.getBlockZ());
                statement.setInt(DOOR_ENG_X, engine.getBlockX());
                statement.setInt(DOOR_ENG_Y, engine.getBlockY());
                statement.setInt(DOOR_ENG_Z, engine.getBlockZ());
                statement.setInt(DOOR_LOCKED, isLocked ? 1 : 0);
                statement.setInt(DOOR_TYPE, DoorType.getValue(type));
                statement.setInt(DOOR_ENG_SIDE, engineSide == null ? -1 : DoorDirection.getValue(engineSide));
                statement.setInt(DOOR_POWER_X, engine.getBlockX());
                statement.setInt(DOOR_POWER_Y, engine.getBlockY() - 1);
                statement.setInt(DOOR_POWER_Z, engine.getBlockZ());
                statement.setInt(DOOR_OPEN_DIR, RotateDirection.getValue(openDir));
                statement.setInt(DOOR_AUTO_CLOSE, autoClose);
                statement.setLong(DOOR_CHUNK_HASH, Util.chunkHashFromLocation(powerBlock.getBlockX(), powerBlock.getBlockZ(), worldUID));
                statement.setLong(DOOR_BLOCKS_TO_MOVE, blocksToMove);
                statement.setInt(DOOR_NOTIFY, notify ? 1 : 0);
                statement.setInt(DOOR_BYPASS_PROTECTIONS, bypassProtections ? 1 : 0);
                statement.executeUpdate();
                try (var resultSet = statement.getGeneratedKeys()) {
                    resultSet.next();
                    return resultSet.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    // Virtual door save/load methods

    public int saveVirtualDoors(World sourceWorld, String saveName) {
        var saveWorldUID = getVirtualWorldUUID(saveName);
        var doorsCount = 0;
        deleteInWorldRaw(saveWorldUID);
        for (var door : getDoorsInWorld(sourceWorld)) {
            insertRaw(
                    VIRTUAL_DOOR_OWNER_UUID,
                    VIRTUAL_DOOR_OWNER_NAME,
                    VIRTUAL_DOOR_OWNER_UUID,
                    saveWorldUID,
                    door.getMinimum(),
                    door.getMaximum(),
                    door.getEngine(),
                    door.getName(),
                    door.isOpen(),
                    door.getDoorUID(),
                    door.isLocked(),
                    0,
                    door.getType(),
                    door.getEngSide(),
                    door.getPowerBlockLoc(),
                    door.getOpenDir(),
                    door.getAutoClose(),
                    door.notificationEnabled(),
                    door.bypassProtections(),
                    door.getBlocksToMove()
            );
            doorsCount++;
        }
        return doorsCount;
    }

    public int loadVirtualDoors(String saveName, World targetWorld, boolean addWorldSuffix) {
        var saveWorldUID = getVirtualWorldUUID(saveName);
        var doorsCount = 0;
        try (var connection = getSQLConnection()) {
            try (var preparedStatement = connection.prepareStatement("SELECT * FROM doors WHERE world = ?")) {
                preparedStatement.setString(1, saveWorldUID.toString());
                try (var resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        var min = new Location(targetWorld, resultSet.getInt(DOOR_MIN_X), resultSet.getInt(DOOR_MIN_Y), resultSet.getInt(DOOR_MIN_Z));
                        var max = new Location(targetWorld, resultSet.getInt(DOOR_MAX_X), resultSet.getInt(DOOR_MAX_Y), resultSet.getInt(DOOR_MAX_Z));
                        var engine = new Location(targetWorld, resultSet.getInt(DOOR_ENG_X), resultSet.getInt(DOOR_ENG_Y), resultSet.getInt(DOOR_ENG_Z));
                        var powerBlock = new Location(targetWorld, resultSet.getInt(DOOR_POWER_X), resultSet.getInt(DOOR_POWER_Y), resultSet.getInt(DOOR_POWER_Z));

                        var name = resultSet.getString(DOOR_NAME);
                        if (addWorldSuffix) {
                            name = name + "_" + targetWorld.getName();
                        }

                        var door = new Door(
                                VIRTUAL_DOOR_OWNER_UUID,
                                VIRTUAL_DOOR_OWNER_NAME,
                                VIRTUAL_DOOR_OWNER_UUID,
                                targetWorld,
                                min,
                                max,
                                engine,
                                name, // we need suffix to make it unique
                                (resultSet.getInt(DOOR_OPEN) == 1),
                                resultSet.getInt(DOOR_ID),
                                (resultSet.getInt(DOOR_LOCKED) == 1),
                                0,
                                DoorType.valueOf(resultSet.getInt(DOOR_TYPE)),
                                DoorDirection.valueOf(resultSet.getInt(DOOR_ENG_SIDE)),
                                powerBlock,
                                RotateDirection.valueOf(resultSet.getInt(DOOR_OPEN_DIR)),
                                resultSet.getInt(DOOR_AUTO_CLOSE),
                                resultSet.getBoolean(DOOR_NOTIFY),
                                resultSet.getBoolean(DOOR_BYPASS_PROTECTIONS)
                        );
                        door.setBlocksToMove(resultSet.getInt(DOOR_BLOCKS_TO_MOVE));

                        virtualDoors.put(door.getDoorUID(), door);
                        virtualDoorsByName.put(door.getName(), door);

                        doorsCount++;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return doorsCount;
    }

    // Overrides

    @Override
    public int getPermission(String playerUUID, long doorUID) {
        if (doorUID >= 0) {
            return super.getPermission(playerUUID, doorUID);
        }
        return -1;
    }

    @Nullable
    @Override
    public Door removeDoor(long doorID) {
        if (doorID >= 0) {
            return super.removeDoor(doorID);
        }
        var door = virtualDoors.remove(doorID);
        if (door == null) {
            return null;
        }
        virtualDoorsByName.remove(door.getName());
        return door;
    }

    @Override
    public List<Door> removeDoorsFromWorld(World world) {
        if (!BigDoorsDungeons.isDungeonWorld(world)) {
            return super.removeDoorsFromWorld(world);
        }
        var removedDoors = new ArrayList<Door>();
        virtualDoors.values().removeIf(door -> {
            if (door.getWorld().equals(world)) {
                removedDoors.add(door);
                virtualDoorsByName.remove(door.getName());
                return true;
            }
            return false;
        });
        return removedDoors;
    }

    @Override
    public Door getDoor(@Nullable UUID playerUUID, long doorUID, boolean includeNonOwners) {
        if (doorUID >= 0) {
            return super.getDoor(playerUUID, doorUID, includeNonOwners);
        }
        return virtualDoors.get(doorUID);
    }

    @Override
    public Set<Door> getDoors() {
        var doors = new HashSet<>(super.getDoors());
        doors.addAll(virtualDoors.values());
        return doors;
    }

    @Override
    public ArrayList<Door> getDoors(String name) {
        var doors = new ArrayList<>(super.getDoors(name));
        doors.addAll(virtualDoors.values().stream().filter(door -> door.getName().equals(name)).toList());
        return doors;
    }

    @Override
    public ArrayList<Door> getDoors(String playerUUIDStr, String name, long start, long end) {
        var doors = super.getDoors(playerUUIDStr, name, start, end);
        var onlinePlayer = Bukkit.getPlayer(UUID.fromString(playerUUIDStr));
        if (onlinePlayer != null && onlinePlayer.hasPermission("bigdoorsdungeons.admin")) {
            var matchedVirtualDoors = virtualDoors.values().stream()
                    .filter(door -> name == null || door.getName().equals(name))
                    .toList();
            doors.addAll(matchedVirtualDoors);
        }
        return doors;
    }

    @Override
    public ArrayList<Door> getDoorsInWorld(World world) {
        if (!BigDoorsDungeons.isDungeonWorld(world)) {
            return super.getDoorsInWorld(world);
        }
        return (ArrayList<Door>) virtualDoors.values()
                .stream()
                .filter(door -> door.getWorld().equals(world))
                .collect(Collectors.toList());
    }

    /* UUID <-> name related methods, not needed
    @Override
    public void updatePlayerName(UUID playerUUID, String playerName) {
        super.updatePlayerName(playerUUID, playerName);
    }

    @Override
    public UUID getUUIDFromName(String playerName) {
        return super.getUUIDFromName(playerName);
    }

    @Override
    public String getPlayerName(UUID playerUUID) {
        return super.getPlayerName(playerUUID);
    }
    */

    @Override
    public DoorOwner getOwnerOfDoor(long doorUID) {
        if (doorUID >= 0) {
            return super.getOwnerOfDoor(doorUID);
        }
        var door = virtualDoors.get(doorUID);
        if (door == null) {
            return null;
        }
        return new DoorOwner(BigDoors.get(), doorUID, VIRTUAL_DOOR_OWNER_UUID, 0, VIRTUAL_DOOR_OWNER_NAME);
    }

    /* Power blocks are unsupported for virtual doors
    @Override
    public HashMap<Long, Long> getPowerBlockData(long chunkHash) {
        return super.getPowerBlockData(chunkHash);
    }

    @Override
    public void recalculatePowerBlockHashes() {
        super.recalculatePowerBlockHashes();
    }
    */

    @Override
    public void updateDoorBlocksToMove(long doorID, int blocksToMove) {
        if (doorID >= 0) {
            super.updateDoorBlocksToMove(doorID, blocksToMove);
            return;
        }
        var door = virtualDoors.get(doorID);
        if (door == null) {
            return;
        }
        door.setBlocksToMove(blocksToMove);
    }

    @Override
    public void updateDoorCoords(long doorID, boolean isOpen, int xMin, int yMin, int zMin, int xMax, int yMax, int zMax, DoorDirection engSide) {
        if (doorID < 0) {
            // Already updated in-memory
            return;
        }
        super.updateDoorCoords(doorID, isOpen, xMin, yMin, zMin, xMax, yMax, zMax, engSide);
    }

    @Override
    public void updateDoorAutoClose(long doorID, int autoClose) {
        if (doorID >= 0) {
            super.updateDoorAutoClose(doorID, autoClose);
            return;
        }
        var door = virtualDoors.get(doorID);
        if (door == null) {
            return;
        }
        try {
            FieldUtils.writeDeclaredField(door, "autoClose", autoClose, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateNotify(long doorUID, boolean notify) {
        if (doorUID >= 0) {
            super.updateNotify(doorUID, notify);
            return;
        }
        var door = virtualDoors.get(doorUID);
        if (door == null) {
            return;
        }
        door.setNotificationEnabled(notify);
    }

    @Override
    public void updateBypassProtections(long doorUID, boolean bypassProtections) {
        if (doorUID >= 0) {
            super.updateBypassProtections(doorUID, bypassProtections);
            return;
        }
        var door = virtualDoors.get(doorUID);
        if (door == null) {
            return;
        }
        door.setBypassProtections(bypassProtections);
    }

    @Override
    public void updateDoorOpenDirection(long doorID, RotateDirection openDir) {
        if (doorID >= 0) {
            super.updateDoorOpenDirection(doorID, openDir);
            return;
        }
        var door = virtualDoors.get(doorID);
        if (door == null) {
            return;
        }
        door.setOpenDir(openDir);
    }

    @Override
    public void updateDoorPowerBlockLoc(long doorID, int xPos, int yPos, int zPos, UUID worldUUID) {
        if (doorID < 0) {
            // Power blocks are unsupported in virtual doors
            return;
        }
        super.updateDoorPowerBlockLoc(doorID, xPos, yPos, zPos, worldUUID);
    }

    @Override
    public boolean isPowerBlockLocationEmpty(Location loc) {
        if (!BigDoorsDungeons.isDungeonWorld(loc.getWorld())) {
            return super.isPowerBlockLocationEmpty(loc);
        }
        return true; // Power blocks are unsupported in virtual doors
    }

    @Override
    public void setLock(long doorID, boolean newLockStatus) {
        if (doorID >= 0) {
            super.setLock(doorID, newLockStatus);
            return;
        }
        var door = virtualDoors.get(doorID);
        if (door == null) {
            return;
        }
        door.setLock(newLockStatus);
    }

    @Override
    public long insert(Door door) {
        if (!BigDoorsDungeons.isDungeonWorld(door.getWorld())) {
            return super.insert(door);
        }
        door = new Door(door.getPrimeOwner(), door.getPlayerName(), door.getPrimeOwner(), door.getWorld(), door.getMinimum(), door.getMaximum(), door.getEngine(), door.getName(), door.isOpen(), lastVirtualDoorId.decrementAndGet(), door.isLocked(), door.getPermission(), door.getType(), door.getLookingDir(), door.getPowerBlockLoc(), door.getOpenDir(), door.getAutoClose(), door.notificationEnabled(), door.bypassProtections());
        virtualDoors.put(door.getDoorUID(), door);
        virtualDoorsByName.put(door.getName(), door);
        return door.getDoorUID();
    }

    @Override
    public boolean removeOwner(long doorUID, UUID playerUUID) {
        if (doorUID >= 0) {
            return super.removeOwner(doorUID, playerUUID);
        }
        return false;
    }

    @Override
    public ArrayList<DoorOwner> getOwnersOfDoor(long doorUID, @Nullable UUID playerUUID) {
        if (doorUID >= 0) {
            return super.getOwnersOfDoor(doorUID, playerUUID);
        }
        return new ArrayList<>();
    }

    @Override
    public void addOwner(long doorUID, UUID playerUUID, int permission) {
        if (doorUID < 0) {
            return;
        }
        super.addOwner(doorUID, playerUUID, permission);
    }

    /*
    @Override
    public long countDoors(String playerUUID, String name) {
        return super.countDoors(playerUUID, name);
    }
    */
}
