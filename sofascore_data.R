# Instalar librerías
options(install.packages.compile.from.source = "never")
install.packages("websocket", type = "binary")
install.packages("chromote",  type = "binary")


options(install.packages.compile.from.source = "never")
install.packages("tidyr", type = "binary")


# ====== Librerúas ======
library(chromote)
library(jsonlite)
library(dplyr)
library(tidyr)
library(stringr)

# ====== Parámetros ======
match_id <- 14056123
match_url <- sprintf("https://www.sofascore.com/football/match/club-america-cruz-azul/CvecsDxec#id:%s", match_id)

# ====== Abrir navegador headless / UI con ruta ya configurada ======
# Si ya fijaste options(chromote.chrome_path=...), no necesitas 'path ='
b <- ChromoteSession$new()
on.exit({
  try(b$close(), silent = TRUE)
  try(b$Browser$close(), silent = TRUE)
}, add = TRUE)

b$Page$navigate(match_url)
# espera a que cargue la app; si tu red es lenta, sube a 8-10 seg
Sys.sleep(7)

# ====== JavaScript que se ejecuta DENTRO de la página ======
# - Hace fetch a lineups y luego a stats por jugadora
# - Devuelve un JSON grande con { players: [...], stats: [...] }
js <- sprintf("
(async () => {
  const matchId = %s;
  const base = 'https://api.sofascore.com/api/v1';

  // Helper: fetch JSON con backoff ligero
  async function getJSON(url, tries = 4) {
    let lastErr;
    for (let i=0; i<tries; i++) {
      try {
        const resp = await fetch(url, { credentials: 'omit' });
        if (resp.ok) return await resp.json();
        lastErr = new Error('HTTP ' + resp.status + ' for ' + url);
        await new Promise(r => setTimeout(r, 500 * (i+1)));
      } catch (e) {
        lastErr = e;
        await new Promise(r => setTimeout(r, 500 * (i+1)));
      }
    }
    throw lastErr;
  }

  // 1) Alineaciones -> playerIds
  const lineupsUrl = `${base}/event/${matchId}/lineups`;
  const lineups = await getJSON(lineupsUrl);

  function flattenSide(sideObj, side) {
    if (!sideObj || !Array.isArray(sideObj.players)) return [];
    return sideObj.players.map(p => ({
      teamSide: side,
      teamName: (sideObj.team && sideObj.team.name) || null,
      coach: (sideObj.coach && sideObj.coach.name) || null,
      playerId: (p.player && p.player.id) || p.id || null,
      playerName: (p.player && p.player.name) || p.name || null,
      position: p.detailedPosition || p.position || null,
      shirtNumber: p.shirtNumber || null,
      isCaptain: !!p.captain,
      isSubstitute: !!p.substitute
    }));
  }

  const players = [
    ...flattenSide(lineups.home, 'home'),
    ...flattenSide(lineups.away, 'away'),
  ].filter(x => x.playerId != null);

  // 2) Stats por jugadora
  async function getPlayerStats(pid) {
    const url = `${base}/event/${matchId}/player/${pid}/statistics`;
    const js = await getJSON(url);
    const arr = [];
    if (js && js.statistics && Array.isArray(js.statistics)) {
      for (const grp of js.statistics) {
        const items = grp.statisticsItems || [];
        for (const it of items) {
          const metric = (it.type) ? it.type
                        : (it.name ? it.name.toLowerCase().replace(/\\s+/g,'_') : null);
          const value = (it.value != null) ? it.value
                       : (it.home != null ? it.home
                       : (it.away != null ? it.away : null));
          if (metric) {
            arr.push({ metric, value });
          }
        }
      }
    }
    return { playerId: pid, statistics: arr };
  }

  // Para no saturar: pequeñas tandas (concurrencia ~6)
  const ids = [...new Set(players.map(p => p.playerId))];
  const chunk = 6;
  const allStats = [];
  for (let i = 0; i < ids.length; i += chunk) {
    const slice = ids.slice(i, i + chunk);
    const got = await Promise.all(slice.map(getPlayerStats));
    allStats.push(...got);
    await new Promise(r => setTimeout(r, 400)); // respiro
  }

  return JSON.stringify({ players, stats: allStats });
})()
", match_id)

res <- b$Runtime$evaluate(expression = js, awaitPromise = TRUE, returnByValue = TRUE)
raw <- fromJSON(res$result$value, flatten = TRUE)

# ====== Limpieza y armado en R ======
players_df <- as_tibble(raw$players) %>%
  transmute(
    match_id = match_id,
    team_side = teamSide,
    team_name = teamName,
    coach,
    player_id = playerId,
    player_name = playerName,
    position,
    shirt = shirtNumber,
    is_captain = isCaptain,
    is_substitute = isSubstitute
  ) %>% distinct(player_id, .keep_all = TRUE)

stats_long <- NULL
if (!is.null(raw$stats) && nrow(players_df)) {
  stats_long <- tibble(
    player_id = unlist(lapply(raw$stats$playerId, function(x) rep(x, length(raw$stats$statistics[[which(raw$stats$playerId == x)[1]]])))),
    metric    = unlist(lapply(raw$stats$statistics, function(lst) vapply(lst, function(z) z$metric %||% NA_character_, ""))),
    value     = suppressWarnings(as.numeric(unlist(lapply(raw$stats$statistics, function(lst) vapply(lst, function(z) z$value %||% NA_real_, 0)))))
  )
  # En algunos casos la línea anterior puede dar warning si hay listas vacías; es normal.
  stats_long <- stats_long %>% filter(!is.na(metric))
}

player_stats_wide <- players_df
if (!is.null(stats_long) && nrow(stats_long)) {
  stats_wide <- stats_long %>%
    mutate(metric = gsub("^_+|_+$", "", tolower(metric))) %>%
    filter(nchar(metric) > 0) %>%
    distinct(player_id, metric, .keep_all = TRUE) %>%
    pivot_wider(names_from = metric, values_from = value)
  
  player_stats_wide <- players_df %>%
    left_join(stats_wide, by = "player_id")
}

# ====== Orden sugerido de columnas (si existen) ======
order_hint <- c("match_id","team_side","team_name","player_name","shirt","position","is_substitute","is_captain",
                "minutesplayed","rating","goals","assists","xg","xa",
                "shotstotal","shotson_target","keypasses",
                "passestotal","passesaccurate",
                "dribbleswon","tackles","interceptions",
                "duelstotal","duelswon","touches","fouls","offsides","saves")
have <- intersect(order_hint, tolower(names(player_stats_wide)))
player_stats_wide <- player_stats_wide %>%
  rename_with(tolower) %>%
  select(any_of(have), everything())

# ====== Resultado y export ======
print(player_stats_wide, n = 50)

# Guarda CSV (ajusta ruta si quieres)
out_csv <- sprintf("sofascore_player_stats_match_%s.csv", match_id)
write.csv(player_stats_wide, out_csv, row.names = FALSE)
message("Archivo guardado: ", normalizePath(out_csv))
