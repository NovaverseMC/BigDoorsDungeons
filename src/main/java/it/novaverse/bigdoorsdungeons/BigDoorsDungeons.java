package it.novaverse.bigdoorsdungeons;

import net.playavalon.mythicdungeons.MythicDungeons;
import net.playavalon.mythicdungeons.dungeons.Dungeon;
import net.playavalon.mythicdungeons.dungeons.Instance;
import nl.pim16aap2.bigDoors.BigDoors;
import nl.pim16aap2.bigDoors.storage.sqlite.SQLiteJDBCDriverConnection;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bukkit.World;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.world.WorldLoadEvent;
import org.bukkit.event.world.WorldUnloadEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Collection;

public final class BigDoorsDungeons extends JavaPlugin implements Listener {

    private HookedDoorStorage hookedDoorStorage;
    private BigDoorsDungeonsPlaceholders placeholders;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        reloadConfig();

        hookedDoorStorage = new HookedDoorStorage(this, BigDoors.get(), BigDoors.get().getConfigLoader().dbFile());
        try {
            FieldUtils.writeDeclaredField(BigDoors.get(), "db", hookedDoorStorage, true);
            FieldUtils.writeDeclaredField(BigDoors.get().getCommander(), "db", hookedDoorStorage, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        getServer().getPluginManager().registerEvents(this, this);

        if (getServer().getPluginManager().isPluginEnabled("PlaceholderAPI")) {
            try {
                placeholders = new BigDoorsDungeonsPlaceholders(this, hookedDoorStorage);
                placeholders.register();
            } catch (Throwable t) {
                getSLF4JLogger().error("Unable to hook into PlaceholderAPI!", t);
            }
        }
    }

    @Override
    public void onDisable() {
        if (placeholders != null) {
            try {
                placeholders.unregister();
            } catch (Throwable t) {
                getSLF4JLogger().error("Unable to unregister PlaceholderAPI expansion...", t);
            }
        }

        try {
            var db = new SQLiteJDBCDriverConnection(BigDoors.get(), BigDoors.get().getConfigLoader().dbFile());
            FieldUtils.writeDeclaredField(BigDoors.get(), "db", db, true);
            FieldUtils.writeDeclaredField(BigDoors.get().getCommander(), "db", new SQLiteJDBCDriverConnection(BigDoors.get(), BigDoors.get().getConfigLoader().dbFile()), true);
            BigDoors.get().getPBCache().reinit(BigDoors.get().getConfigLoader().cacheTimeout()); // Re-init cache
        } catch (Throwable t) {
            getSLF4JLogger().error("Unable to restore default SQL storage...", t);
        }

        hookedDoorStorage = null;
        placeholders = null;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onWorldLoad(WorldLoadEvent event) {
        var world = event.getWorld();

        getServer().getScheduler().runTaskLater(this, () -> {
            if (getServer().getWorld(world.getUID()) == null) {
                return;
            }

            var instance = getDungeonInstance(world);
            if (instance == null) {
                return;
            }

            var editMode = instance.isEditMode();
            var dungeonName = instance.getDungeon().getWorldName();

            var count = hookedDoorStorage.loadVirtualDoors(dungeonName, world, editMode);
            getSLF4JLogger().info("Loaded dungeon " + world.getName() + " doors! (" + count + ")");
        }, 20);
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = true)
    public void onWorldUnload(WorldUnloadEvent event) {
        var world = event.getWorld();

        var instance = getDungeonInstance(world);
        if (instance == null) {
            return;
        }
        var dungeon = instance.getDungeon();
        var editMode = instance.isEditMode();

        if (editMode) {
            var count = hookedDoorStorage.saveVirtualDoors(world, dungeon.getWorldName());
            getSLF4JLogger().info("Saved dungeon " + world.getName() + " doors! (" + count + ")");
        }

        getSLF4JLogger().info("Removing doors from dungeon world " + world.getName());
        var cache = BigDoors.get().getPBCache();
        hookedDoorStorage.removeDoorsFromWorld(world)
                .forEach(door -> cache.invalidate(door.getDoorUID()));
    }

    public static Instance getDungeonInstance(World world) {
        return MythicDungeons.inst().getDungeons().getAll().stream()
                .map(Dungeon::getInstances)
                .flatMap(Collection::stream)
                .filter(current -> world.equals(current.getInstanceWorld()))
                .findAny()
                .orElse(null);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isDungeonWorld(World world) {
        return getDungeonInstance(world) != null;
    }
}
