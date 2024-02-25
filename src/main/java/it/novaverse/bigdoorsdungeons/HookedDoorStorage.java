package it.novaverse.bigdoorsdungeons;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import nl.pim16aap2.bigDoors.BigDoors;
import nl.pim16aap2.bigDoors.Door;
import nl.pim16aap2.bigDoors.storage.sqlite.SQLiteJDBCDriverConnection;
import nl.pim16aap2.bigDoors.util.DoorDirection;
import nl.pim16aap2.bigDoors.util.DoorOwner;
import nl.pim16aap2.bigDoors.util.DoorType;
import nl.pim16aap2.bigDoors.util.RotateDirection;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HookedDoorStorage extends SQLiteJDBCDriverConnection {

    private final static UUID VIRTUAL_DOOR_OWNER_UUID = UUID.fromString("a8190a78-cf83-11ee-a506-0242ac120002");
    private final static String VIRTUAL_DOOR_OWNER_NAME = "BigDoorsDungeons";

    private final BigDoorsDungeons plugin;

    private final Map<Long, Door> virtualDoors;

    private final Map<String, Door> virtualDoorsByName;
    private final AtomicLong lastVirtualDoorId;

    public HookedDoorStorage(BigDoorsDungeons plugin, BigDoors bigDoors, String dbName) {
        super(bigDoors, dbName);
        this.plugin = plugin;
        virtualDoors = new Long2ObjectOpenHashMap<>();
        virtualDoorsByName = new LinkedHashMap<>();
        lastVirtualDoorId = new AtomicLong(-1);
    }

    public Door getVirtualDoorByName(String name) {
        return virtualDoorsByName.get(name);
    }

    // Virtual door save/load methods

    public int saveVirtualDoors(World sourceWorld, String saveName) {
        var worldsSection = plugin.getConfig().getConfigurationSection("worlds");
        if (worldsSection == null) {
            worldsSection = plugin.getConfig().createSection("worlds");
        }
        worldsSection.set(saveName, null);

        var worldSection = worldsSection.createSection(saveName);

        var doorsCount = 0;
        for (var door : getDoorsInWorld(sourceWorld)) {
            var doorSection = worldSection.createSection(door.getName());
            doorSection.set("min", door.getMinimum().toVector());
            doorSection.set("max", door.getMaximum().toVector());
            doorSection.set("engine", door.getEngine().toVector());
            doorSection.set("powerBlock", door.getPowerBlockLoc().toVector());
            doorSection.set("open", door.isOpen());
            doorSection.set("locked", door.isLocked());
            doorSection.set("type", door.getType().name());
            doorSection.set("engSide", door.getEngSide().name());
            doorSection.set("openDir", door.getOpenDir().name());
            doorSection.set("autoClose", door.getAutoClose());
            doorSection.set("notificationEnabled", door.notificationEnabled());
            doorSection.set("bypassProtections", door.bypassProtections());
            doorSection.set("blocksToMove", door.getBlocksToMove());
            doorsCount++;
        }

        plugin.saveConfig();

        return doorsCount;
    }

    public int loadVirtualDoors(String saveName, World targetWorld, boolean editMode) {
        var worldsSection = plugin.getConfig().getConfigurationSection("worlds");
        if (worldsSection == null) {
            return 0;
        }
        var worldSection = worldsSection.getConfigurationSection(saveName);
        if (worldSection == null) {
            return 0;
        }

        var doorsCount = 0;

        for (var doorSectionKey : worldSection.getKeys(false)) {
            var doorSection = Objects.requireNonNull(worldSection.getConfigurationSection(doorSectionKey));

            var min = Objects.requireNonNull(doorSection.getVector("min")).toLocation(targetWorld);
            var max = Objects.requireNonNull(doorSection.getVector("max")).toLocation(targetWorld);
            var engine = Objects.requireNonNull(doorSection.getVector("engine")).toLocation(targetWorld);
            var powerBlock = Objects.requireNonNull(doorSection.getVector("powerBlock")).toLocation(targetWorld);

            var name = doorSectionKey;
            if (!editMode) {
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
                    doorSection.getBoolean("open"),
                    lastVirtualDoorId.decrementAndGet(), // Door id
                    doorSection.getBoolean("locked"),
                    0,
                    DoorType.valueOf(doorSection.getString("type")),
                    DoorDirection.valueOf(doorSection.getString("engSide")),
                    powerBlock,
                    RotateDirection.valueOf(doorSection.getString("openDir")),
                    doorSection.getInt("autoClose"),
                    doorSection.getBoolean("notify"),
                    doorSection.getBoolean("bypassProtections")
            );
            door.setBlocksToMove(doorSection.getInt("blocksToMove"));

            virtualDoors.put(door.getDoorUID(), door);
            virtualDoorsByName.put(door.getName(), door);

            doorsCount++;
        }

        return doorsCount;
    }

    // Overrides

    @Override
    public int getPermission(String playerUUID, long doorUID) {
        if (doorUID >= 0) {
            return super.getPermission(playerUUID, doorUID);
        }
        var onlinePlayer = Bukkit.getPlayer(UUID.fromString(playerUUID));
        if (onlinePlayer != null && onlinePlayer.hasPermission("bigdoorsdungeons.admin")) {
            return 0;
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

    /* FIXME: implement for virtual doors
    @Override
    public HashMap<Long, Long> getPowerBlockData(long chunkHash) {
        return super.getPowerBlockData(chunkHash);
    }
    */

    /* No need to recalculate hashes for virtual doors
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

    /* Virtual doors shouldn't be counted in the player door count
    @Override
    public long countDoors(String playerUUID, String name) {
        return super.countDoors(playerUUID, name);
    }
    */
}
