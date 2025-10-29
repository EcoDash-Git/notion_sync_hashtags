#!/usr/bin/env Rscript

# ─────────────────────────────────────────────────────────────────────────────
# Supabase (Postgres) → Notion (paged, resumable)
# Source table: twitter_hashtags
# Unique key per row: (tweet_id, tag)
# ─────────────────────────────────────────────────────────────────────────────

# --- packages ----------------------------------------------------------------
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ------------------------------------------------------------------
NOTION_TOKEN       <- Sys.getenv("NOTION_TOKEN")
DB_ID              <- Sys.getenv("NOTION_DATABASE_ID")
LOOKBACK_HOURS     <- as.integer(Sys.getenv("LOOKBACK_HOURS","17532")) # ~2 years
NOTION_VERSION     <- "2022-06-28"

NUM_NA_AS_ZERO     <- tolower(Sys.getenv("NUM_NA_AS_ZERO","false")) %in% c("1","true","yes")
INSPECT_FIRST_ROW  <- tolower(Sys.getenv("INSPECT_FIRST_ROW","false")) %in% c("1","true","yes")
DUMP_SCHEMA        <- tolower(Sys.getenv("DUMP_SCHEMA","false"))       %in% c("1","true","yes")
RATE_DELAY_SEC     <- as.numeric(Sys.getenv("RATE_DELAY_SEC","0.20"))
IMPORT_ALL         <- tolower(Sys.getenv("IMPORT_ALL","false"))        %in% c("1","true","yes")
RUN_SMOKE_TEST     <- tolower(Sys.getenv("RUN_SMOKE_TEST","false"))    %in% c("1","true","yes")

CHUNK_SIZE         <- as.integer(Sys.getenv("CHUNK_SIZE","800"))
CHUNK_OFFSET       <- as.integer(Sys.getenv("CHUNK_OFFSET","0"))
ORDER_DIR          <- toupper(Sys.getenv("ORDER_DIR","DESC"))
if (!(ORDER_DIR %in% c("ASC","DESC"))) ORDER_DIR <- "DESC"

# Resumability budgets (stop early; workflow will auto-continue)
MAX_ROWS_PER_RUN   <- as.integer(Sys.getenv("MAX_ROWS_PER_RUN","1200"))
MAX_MINUTES        <- as.numeric(Sys.getenv("MAX_MINUTES","110"))
t0 <- Sys.time()

if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")

# --- helpers -----------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list()
  else list(list(type="text", text=list(content=substr(s, 1, 1800))))
}

parse_dt <- function(x) {
  if (inherits(x,"POSIXt")) return(as.POSIXct(x, tz="UTC"))
  if (inherits(x,"Date"))   return(as.POSIXct(x, tz="UTC"))
  if (inherits(x,"integer64")) x <- as.character(x)
  if (is.numeric(x))        return(as.POSIXct(x, origin="1970-01-01", tz="UTC"))
  if (!is.character(x))     return(as.POSIXct(NA, origin="1970-01-01", tz="UTC"))
  xx <- trimws(x)
  fmts <- c("%Y-%m-%dT%H:%M:%OSZ","%Y-%m-%d %H:%M:%S%z","%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S","%Y-%m-%d")
  for (f in fmts) {
    d <- suppressWarnings(as.POSIXct(xx, format=f, tz="UTC"))
    if (!is.na(d)) return(d)
  }
  as.POSIXct(NA, origin="1970-01-01", tz="UTC")
}

perform <- function(req, tag = "", max_tries = 6, base_sleep = 0.5) {
  last <- NULL
  for (i in seq_len(max_tries)) {
    resp <- tryCatch(req_perform(req), error = function(e) e)
    last <- resp
    if (inherits(resp, "httr2_response")) {
      sc <- resp$status_code
      if (sc != 429 && sc < 500) return(resp)
      ra <- resp_headers(resp)[["retry-after"]]
      wait <- if (!is.null(ra)) suppressWarnings(as.numeric(ra)) else base_sleep * 2^(i - 1)
      Sys.sleep(min(wait, 10))
    } else {
      Sys.sleep(base_sleep * 2^(i - 1))
    }
  }
  err <- list(
    tag = tag,
    status = if (inherits(last,"httr2_response")) last$status_code else NA_integer_,
    body = if (inherits(last,"httr2_response")) tryCatch(resp_body_string(last), error=function(...) "<no body>")
    else paste("R error:", conditionMessage(last))
  )
  structure(list(.err = TRUE, err = err), class = "notion_err")
}
is_err   <- function(x) inherits(x, "notion_err") && isTRUE(x$.err %||% TRUE)
show_err <- function(x, row_i = NA, key = NA) {
  if (!is_err(x)) return(invisible())
  er <- x$err
  cat(sprintf("⚠️ Notion error%s%s [%s] Status: %s\nBody: %s\n",
              if (!is.na(row_i)) paste0(" on row ", row_i) else "",
              if (!is.na(key))   paste0(" (key=", key, ")") else "",
              er$tag %||% "request",
              as.character(er$status %||% "n/a"),
              er$body %||% "<empty>"))
}

notion_req <- function(url) {
  request(url) |>
    req_headers(
      Authorization    = paste("Bearer", NOTION_TOKEN),
      "Notion-Version" = NOTION_VERSION,
      "Content-Type"   = "application/json"
    )
}

# --- Notion schema -----------------------------------------------------------
get_db_schema <- function() {
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID)) |> perform(tag="GET /databases")
  if (is_err(resp)) { show_err(resp); stop("Could not read Notion database schema.") }
  resp_body_json(resp, simplifyVector = FALSE)
}
.DB <- get_db_schema()
PROPS <- .DB$properties
TITLE_PROP <- names(Filter(function(p) identical(p$type, "title"), PROPS))[1]
if (is.null(TITLE_PROP)) stop("This Notion database has no Title (Name) property.")
if (DUMP_SCHEMA) {
  cat("\n--- Notion schema ---\n")
  cat(paste(
    vapply(names(PROPS), function(n) sprintf("%s : %s", n, PROPS[[n]]$type), character(1)),
    collapse = "\n"
  ), "\n----------------------\n")
}

to_num <- function(x) {
  if (inherits(x, "integer64")) x <- as.character(x)
  v <- suppressWarnings(as.numeric(x))
  if (is.na(v) && NUM_NA_AS_ZERO) v <- 0
  v
}

set_prop <- function(name, value) {
  p <- PROPS[[name]]; if (is.null(p)) return(NULL)
  tp <- p$type
  if (tp == "title") {
    list(title = list(list(type="text", text=list(content=as.character(value %||% "untitled")))))
  } else if (tp == "rich_text") {
    list(rich_text = rtxt(value))
  } else if (tp == "number") {
    v <- to_num(value); if (!is.finite(v)) return(NULL)
    list(number = v)
  } else if (tp == "url") {
    u <- as.character(value %||% ""); if (!nzchar(u)) return(list(url = NULL))
    list(url = u)
  } else if (tp == "date") {
    d <- parse_dt(value); if (is.na(d)) return(NULL)
    list(date = list(start = format(d, "%Y-%m-%dT%H:%M:%SZ")))
  } else if (tp == "select") {
    v <- as.character(value %||% ""); if (!nzchar(v)) return(NULL)
    list(select = list(name = v))
  } else NULL
}

# Compose a stable title: "tweet_id | tag"
compose_title <- function(r) {
  paste0(as.character(r$tweet_id %||% "unknown"), " | ", as.character(r$tag %||% ""))
}

props_from_row <- function(r) {
  pr <- list()
  ttl <- compose_title(r)
  pr[[TITLE_PROP]] <- set_prop(TITLE_PROP, ttl)

  # Include tweet_type; silently ignored if Notion DB has no such property
  wanted <- c("tweet_id","tag","tweet_url","publish_dt","username","main_id","text","tweet_type")
  for (nm in wanted) {
    if (!is.null(PROPS[[nm]]) && !is.null(r[[nm]])) pr[[nm]] <- set_prop(nm, r[[nm]])
  }
  pr
}

# --- Batch index by Title (equals) -------------------------------------------
build_index_by_title <- function(titles) {
  by_title <- new.env(parent=emptyenv())
  if (!length(titles)) return(list(by_title = by_title))

  i <- 1L
  while (i <= length(titles)) {
    slice <- titles[i:min(i+30L, length(titles))]  # keep filter size modest
    ors <- lapply(slice, function(tt) list(property = TITLE_PROP, title = list(equals = tt)))
    body <- list(filter = list(or = ors), page_size = 100)
    resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
      req_body_json(body, auto_unbox = TRUE) |>
      perform(tag="POST /databases/query (title batch)")
    if (is_err(resp)) { show_err(resp); i <- i + 31L; next }
    out <- resp_body_json(resp, simplifyVector = FALSE)
    for (pg in out$results) {
      pid <- pg$id
      tnodes <- pg$properties[[TITLE_PROP]]$title
      ttl <- if (length(tnodes)) paste0(vapply(tnodes, \(x) x$plain_text %||% "", character(1L)), collapse = "") else ""
      if (nzchar(ttl)) by_title[[ttl]] <- pid
    }
    i <- i + 31L
    Sys.sleep(RATE_DELAY_SEC/2)
  }

  list(by_title = by_title)
}

# --- CRUD --------------------------------------------------------------------
create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /pages")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  resp_body_json(resp, simplifyVector = TRUE)$id
}
update_page <- function(page_id, pr) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr), auto_unbox = TRUE) |>
    perform(tag="PATCH /pages/:id")
  if (is_err(resp)) return(structure(FALSE, class="notion_err", .err=TRUE, err=resp$err))
  TRUE
}

# --- Upsert (by Title) -------------------------------------------------------
upsert_row <- function(r, idx = NULL) {
  title_val <- compose_title(r)
  pr_full   <- props_from_row(r)

  pid <- NA_character_
  if (!is.null(idx)) {
    pid <- idx$by_title[[title_val]]; if (is.null(pid)) pid <- NA_character_
  } else {
    # Fallback single search
    body <- list(filter = list(property = TITLE_PROP, title = list(equals = title_val)), page_size = 1)
    resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
      req_body_json(body, auto_unbox = TRUE) |>
      perform(tag="POST /databases/query (single)")
    if (!is_err(resp)) {
      out <- resp_body_json(resp, simplifyVector = TRUE)
      if (length(out$results)) pid <- out$results$id[1] else pid <- NA_character_
    }
  }

  if (!is.na(pid[1])) {
    ok <- update_page(pid, pr_full)
    return(is.logical(ok) && ok)
  }

  pid2 <- create_page(pr_full)
  if (!is.na(pid2[1])) return(TRUE)

  # Minimal create with just Title, then patch
  pr_min <- list(); pr_min[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)
  pid3 <- create_page(pr_min)
  if (is.na(pid3[1])) return(FALSE)
  ok2 <- update_page(pid3, pr_full)
  is.logical(ok2) && ok2
}

# --- Supabase connection -----------------------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (!nzchar(supa_host) || !nzchar(supa_user) || !nzchar(supa_pwd)) stop("Set SUPABASE_HOST, SUPABASE_USER, SUPABASE_PWD.")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = as.character(Sys.getenv("SUPABASE_DB", "postgres")),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

# Detect presence of tweet_type in twitter_hashtags
HAS_TWEET_TYPE <- tryCatch({
  q <- "
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'twitter_hashtags'
      AND column_name = 'tweet_type'
    LIMIT 1"
  nrow(DBI::dbGetQuery(con, q)) > 0
}, error = function(...) FALSE)

# Build reusable SELECT column list
SQL_COLS <- paste(
  c("tweet_id","tag","tweet_url","publish_dt","username","main_id","text",
    if (HAS_TWEET_TYPE) "tweet_type" else NULL),
  collapse = ", "
)

message(sprintf("tweet_type column detected: %s", if (HAS_TWEET_TYPE) "YES" else "NO"))

since <- as.POSIXct(Sys.time(), tz="UTC") - LOOKBACK_HOURS * 3600
since_str <- format(since, "%Y-%m-%d %H:%M:%S%z")

# Base WHERE (lookback by publish_dt) or ALL
base_where <- if (IMPORT_ALL) "TRUE" else paste0("publish_dt >= TIMESTAMPTZ ", DBI::dbQuoteString(con, since_str))

# For uniqueness, key = tweet_id || '|' || lower(tag)
exp_sql <- sprintf("
  SELECT COUNT(*) AS n FROM (
    SELECT DISTINCT CAST(tweet_id AS TEXT) || '|' || LOWER(COALESCE(tag,'')) AS key
    FROM twitter_hashtags
    WHERE %s
  ) t
", base_where)
expected_raw <- DBI::dbGetQuery(con, exp_sql)$n[1]
expected_num <- suppressWarnings(as.numeric(expected_raw)); if (!is.finite(expected_num)) expected_num <- 0
expected_i <- as.integer(round(expected_num))
message(sprintf("Expected distinct (tweet_id, tag) rows: %d", expected_i))

# Optional smoke test
if (RUN_SMOKE_TEST) {
  smoke_title <- paste0("ping ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  smoke_pid <- create_page(setNames(list(set_prop(TITLE_PROP, smoke_title)), TITLE_PROP))
  if (is_err(smoke_pid) || is.na(smoke_pid[1])) {
    cat("\n🔥 Smoke test FAILED — cannot create even a minimal page in this database.\n",
        "Confirm the integration has access to THIS database.\n", sep = "")
    if (is_err(smoke_pid)) show_err(smoke_pid)
    DBI::dbDisconnect(con); quit(status = 1L, save = "no")
  } else {
    notion_req(paste0("https://api.notion.com/v1/pages/", smoke_pid)) |>
      req_method("PATCH") |>
      req_body_json(list(archived = TRUE), auto_unbox = TRUE) |>
      perform(tag="ARCHIVE ping")
  }
}

# Optional inspector
if (INSPECT_FIRST_ROW) {
  test_q <- sprintf("
    WITH ranked AS (
      SELECT h.*,
             ROW_NUMBER() OVER (
               PARTITION BY CAST(h.tweet_id AS TEXT), LOWER(COALESCE(h.tag,''))
               ORDER BY h.publish_dt DESC NULLS LAST, CAST(h.tweet_id AS TEXT) %s
             ) AS rn
      FROM twitter_hashtags h
      WHERE %s
    )
    SELECT %s
    FROM ranked
    WHERE rn = 1
    ORDER BY publish_dt %s NULLS LAST, CAST(tweet_id AS TEXT) %s
    LIMIT 1 OFFSET %d
  ", ORDER_DIR, base_where, SQL_COLS, ORDER_DIR, ORDER_DIR, CHUNK_OFFSET)
  one <- DBI::dbGetQuery(con, test_q)
  if (nrow(one)) {
    if (!is.null(one$publish_dt)) one$publish_dt <- parse_dt(one$publish_dt)
    print(one)
  }
}

# --- Main loop ---------------------------------------------------------------
offset <- CHUNK_OFFSET
total_success <- 0L
total_seen    <- 0L

repeat {
  qry <- sprintf("
    WITH ranked AS (
      SELECT h.*,
             ROW_NUMBER() OVER (
               PARTITION BY CAST(h.tweet_id AS TEXT), LOWER(COALESCE(h.tag,''))
               ORDER BY h.publish_dt DESC NULLS LAST, CAST(h.tweet_id AS TEXT) %s
             ) AS rn
      FROM twitter_hashtags h
      WHERE %s
    )
    SELECT %s
    FROM ranked
    WHERE rn = 1
    ORDER BY publish_dt %s NULLS LAST, CAST(tweet_id AS TEXT) %s
    LIMIT %d OFFSET %d
  ", ORDER_DIR, base_where, SQL_COLS, ORDER_DIR, ORDER_DIR, CHUNK_SIZE, offset)

  rows <- DBI::dbGetQuery(con, qry)
  n <- nrow(rows)
  if (!n) break

  message(sprintf("Fetched %d rows (offset=%d).", n, offset))

  # Build a tiny Notion index for this page (by Title)
  titles <- vapply(seq_len(n), function(i) compose_title(rows[i, , drop=FALSE]), character(1))
  idx <- build_index_by_title(titles)

  success <- 0L
  for (i in seq_len(n)) {
    r <- rows[i, , drop = FALSE]
    if (!is.null(r$publish_dt)) {
      d <- parse_dt(r$publish_dt); if (is.na(d)) r$publish_dt <- NULL else r$publish_dt <- d
    }
    key <- compose_title(r)
    message(sprintf("Upserting %s", key))
    ok <- upsert_row(r, idx = idx)
    if (ok) success <- success + 1L else message(sprintf("Row %d failed (key=%s)", i, key))
    if (i %% 50 == 0) message(sprintf("Processed %d/%d in this page (ok %d)", i, n, success))
    Sys.sleep(RATE_DELAY_SEC)
  }

  total_success <- total_success + success
  total_seen    <- total_seen + n
  offset        <- offset + n

  message(sprintf("Page done. %d/%d upserts ok (cumulative ok %d, seen %d of ~%d).",
                  success, n, total_success, total_seen, expected_i))

  # Stop early to avoid hard timeout & chain next run
  elapsed_min <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
  if (total_seen >= MAX_ROWS_PER_RUN || elapsed_min >= MAX_MINUTES) {
    message(sprintf("Stopping early (seen=%d, elapsed=%.1f min). Next offset = %d",
                    total_seen, elapsed_min, offset))
    go <- Sys.getenv("GITHUB_OUTPUT")
    if (nzchar(go)) write(paste0("next_offset=", offset), file = go, append = TRUE)
    DBI::dbDisconnect(con)
    quit(status = 0, save = "no")
  }
}

DBI::dbDisconnect(con)
message(sprintf("All pages done. Upserts ok: %d. Expected distinct: %d", total_success, expected_i))

# Tell the workflow we’re finished
go <- Sys.getenv("GITHUB_OUTPUT")
if (nzchar(go)) write("next_offset=done", file = go, append = TRUE)
