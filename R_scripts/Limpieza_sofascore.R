# ============================================
# Agregado SOLO Club América — robusto
# ============================================

# 0) Archivos
files <- list.files(pattern = "^sofascore.*\\.csv$", full.names = TRUE)
if (!length(files)) stop("No encontré archivos que empiecen con 'sofascore' en esta carpeta.")

# 1) Columnas base (en minúsculas)
expected_cols <- tolower(c(
  "team_side","team_name","player_id","player_name","position","shirt","is_substitute",
  "total_pass","accurate_pass","total_long_balls","accurate_long_balls","goal_assist",
  "total_cross","accurate_cross","aerial_lost","aerial_won","duel_lost","duel_won",
  "challenge_lost","total_contest","won_contest","on_target_scoring_attempt","goals",
  "total_clearance","outfielder_block","total_tackle","error_lead_to_ashot","own_goals",
  "was_fouled","fouls","total_offside","good_high_claim",
  "saved_shots_from_inside_the_box","saves","total_keeper_sweeper","accurate_keeper_sweeper",
  "minutes_played","touches","rating","possession_lost_ctrl","expected_goals","rating_versions",
  "interception_won","penalty_conceded","dispossessed","shot_off_target","key_pass",
  "expected_assists","blocked_scoring_attempt","penalty_won"
))

# 2) Leer cada archivo
read_one <- function(f) {
  df <- try(read.csv(f, stringsAsFactors = FALSE, check.names = FALSE), silent = TRUE)
  if (inherits(df, "try-error")) stop("Error leyendo: ", f)
  names(df) <- tolower(names(df))
  
  # Rellenar team_name -> "América" si está vacío/NA
  if ("team_name" %in% names(df)) {
    na_or_empty <- function(x) is.na(x) | x == ""
    df$team_name[na_or_empty(df$team_name)] <- "América"
  } else {
    df$team_name <- "América"
  }
  
  # Garantizar columnas base (faltantes como NA) y poner primero expected_cols
  miss <- setdiff(expected_cols, names(df))
  if (length(miss)) df[miss] <- NA
  
  df <- df[, c(expected_cols, setdiff(names(df), expected_cols)), drop = FALSE]
  df
}

dfs <- lapply(files, read_one)

# 3) ***Alinear columnas ENTRE archivos***
#    (unión de todos los nombres y rellenar faltantes en cada df)
all_cols <- Reduce(union, lapply(dfs, names))
dfs_aligned <- lapply(dfs, function(d) {
  miss <- setdiff(all_cols, names(d))
  if (length(miss)) d[miss] <- NA
  d[, all_cols, drop = FALSE]
})

# 4) Unir sin errores de columnas
all_df <- do.call(rbind, dfs_aligned)

# 5) Clave de agregación (player_id si existe; si no, player_name)
if (!"player_id" %in% names(all_df)) all_df$player_id <- NA
agg_key <- as.character(all_df$player_id)
agg_key[is.na(agg_key) | agg_key == ""] <- all_df$player_name[is.na(agg_key) | agg_key == ""]
all_df$agg_key <- agg_key

# 6) Forzar tipos numéricos y definir qué sumar / promediar
to_num <- function(x) suppressWarnings(as.numeric(x))

rating_col <- "rating"
if (rating_col %in% names(all_df)) all_df[[rating_col]] <- to_num(all_df[[rating_col]])

id_like <- c("agg_key","player_id","player_name","team_name","team_side","position","shirt","is_substitute")
sum_cols <- setdiff(intersect(expected_cols, names(all_df)), c(id_like, rating_col))

for (cn in sum_cols) all_df[[cn]] <- to_num(all_df[[cn]])
if ("minutes_played" %in% names(all_df)) all_df$minutes_played <- to_num(all_df$minutes_played)

# 7) Agregaciones
# 7a) promedio de rating
if (rating_col %in% names(all_df)) {
  agg_rating <- aggregate(all_df[[rating_col]],
                          by = list(agg_key = all_df$agg_key),
                          FUN = function(x) if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE))
  names(agg_rating)[2] <- "rating_mean"
} else {
  agg_rating <- data.frame(agg_key = unique(all_df$agg_key), rating_mean = NA_real_)
}

# 7b) sumatorias (si no hay columnas a sumar, crea data.frame mínimo)
if (length(sum_cols)) {
  agg_sums_list <- lapply(sum_cols, function(cn)
    aggregate(all_df[[cn]], by = list(agg_key = all_df$agg_key), FUN = function(x) sum(x, na.rm = TRUE)))
  for (i in seq_along(sum_cols)) names(agg_sums_list[[i]])[2] <- sum_cols[i]
  agg_sum_df <- if (length(agg_sums_list) == 1) agg_sums_list[[1]] else
    Reduce(function(a,b) merge(a,b,by="agg_key",all=TRUE), agg_sums_list)
} else {
  agg_sum_df <- data.frame(agg_key = unique(all_df$agg_key))
}

# 7c) atributos (modo/primer no-NA) + conteo de partidos
mode_char <- function(x) {
  x <- x[!is.na(x) & nzchar(as.character(x))]
  if (!length(x)) return(NA_character_)
  names(sort(table(x), decreasing = TRUE))[1]
}
first_non_na <- function(x) {
  ix <- which(!is.na(x) & x != "")
  if (length(ix)) x[ix[1]] else NA
}
pick_attr <- function(x) if (is.numeric(x)) first_non_na(x) else mode_char(as.character(x))

attrs <- intersect(c("player_id","player_name","team_name","team_side","position","shirt","is_substitute"), names(all_df))
agg_ids <- aggregate(all_df[, attrs, drop = FALSE],
                     by = list(agg_key = all_df$agg_key),
                     FUN = pick_attr)
games <- aggregate(list(games = all_df$agg_key), by = list(agg_key = all_df$agg_key), FUN = length)

# 8) Unir todo
agg <- merge(agg_ids, agg_sum_df, by = "agg_key", all = TRUE)
agg <- merge(agg, agg_rating, by = "agg_key", all = TRUE)
agg <- merge(agg, games, by = "agg_key", all = TRUE)

# 9) NAs numéricos -> 0 (excepto rating_mean)
num_cols <- names(agg)[sapply(agg, is.numeric)]
num_cols_no_rating <- setdiff(num_cols, "rating_mean")
for (cn in num_cols_no_rating) agg[[cn]][is.na(agg[[cn]])] <- 0

# 10) (Opcional) per90
per90_from <- intersect(c("goals","goal_assist","key_pass","on_target_scoring_attempt",
                          "total_pass","accurate_pass","total_tackle","interception_won",
                          "duel_won","duel_lost","touches","expected_goals","expected_assists"),
                        names(agg))
if ("minutes_played" %in% names(agg)) {
  denom <- ifelse(agg$minutes_played > 0, agg$minutes_played / 90, NA_real_)
  for (cn in per90_from) {
    newn <- paste0(cn, "_per90")
    agg[[newn]] <- ifelse(is.na(denom), NA_real_, agg[[cn]] / denom)
  }
}

# 11) Orden agradable
nice_first <- c("player_id","player_name","team_name","position","shirt","is_substitute","games","rating_mean","minutes_played",
                "goals","goal_assist","expected_goals","expected_assists","key_pass")
present <- intersect(nice_first, names(agg))
rest <- setdiff(names(agg), c("agg_key", present))
agg <- agg[, c(present, rest, "agg_key")]

# 12) Guardar
out <- "sofascore_america_players_agg.csv"
write.csv(agg, out, row.names = FALSE)
cat("✅ Agregado (solo América) listo en:", normalizePath(out), "\n")
