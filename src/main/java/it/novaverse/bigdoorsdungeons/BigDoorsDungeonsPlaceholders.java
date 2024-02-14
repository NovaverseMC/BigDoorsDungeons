package it.novaverse.bigdoorsdungeons;

import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("UnstableApiUsage")
public class BigDoorsDungeonsPlaceholders extends PlaceholderExpansion {

    private final BigDoorsDungeons plugin;

    public BigDoorsDungeonsPlaceholders(BigDoorsDungeons plugin) {
        this.plugin = plugin;
    }

    @Override
    public @NotNull String getIdentifier() {
        return plugin.getName().toLowerCase();
    }

    @Override
    public @NotNull String getAuthor() {
        return String.join(", ", plugin.getPluginMeta().getAuthors());
    }

    @Override
    public @NotNull String getVersion() {
        return plugin.getPluginMeta().getVersion();
    }

    @Override
    public boolean persist() {
        return true;
    }

    @Override
    public String onRequest(OfflinePlayer offlinePlayer, @NotNull String params) {
        if (!(offlinePlayer instanceof Player player)) {
            return "Offline";
        }

        var instance = BigDoorsDungeons.getDungeonInstance(player.getWorld());
        if (instance == null) {
            return "Not_in_dungeon";
        }

        if (params.endsWith("_id")) {
            var doorName = params.split("_id", 2)[0];
            if (!instance.isEditMode()) {
                doorName = doorName + "_" + player.getWorld().getName();
            }
            var id = plugin.getDungeonDoorId(doorName);
            return String.valueOf(id);
        }

        return null;
    }
}
