suppressPackageStartupMessages(library(DBI))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(glue))

# making these all read from env vars might be the right move at some point
# or maybe from a config file or something
EXPUNGE_DB <- "expunge"
HOST <- "localhost"
PORT <- "5432"
USER <- "jupyter"
PW <- Sys.getenv("POSTGRES_PASS")

# connect to the postgres database
DB_CON <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname = EXPUNGE_DB,
  host = HOST,
  port = PORT,
  password = PW,
  user = USER
)

read_person_file_db <- function(.id, .table) {
    res <- dbSendQuery(DB_CON, glue('
      SELECT
      "person_id",
      "HearingDate",
      "CodeSection",
      "ChargeType",
      "Class",
      "DispositionCode",
      "Plea",
      "Race",
      "Sex",
      "fips"
      FROM {.table} a WHERE person_id = \'{.id}\''))
    df <- dbFetch(res)
    dbClearResult(res)
    return(df)
}


get_ids_from_table <- function(.table) {
    res <- dbSendQuery(DB_CON, glue("SELECT DISTINCT person_id FROM {.table}"))
    all_ids <- dbFetch(res)
    dbClearResult(res)
    return(all_ids$person_id)
}

classify_table <- function(input_table, output_table, classifier_func, update_every = 1000) {
    start <- Sys.time()
    all_ids <- get_ids_from_table(input_table)
    message(glue("Classifying {length(all_ids)} ID's from {input_table} and writing to {output_table}"))
    
    .et <- dbListTables(DB_CON)
    for (.i in 1:length(all_ids)) {
	.id <- all_ids[.i]
        suppressWarnings({ # this is clearly a bad idea but there's a warning on every ID, which is too painful
	xdf <- classifier_func(read_person_file_db(.id, input_table))
	if (.i==1 & !(output_table %in% .et)) {
	    dbWriteTable(DB_CON, output_table, xdf)
	} else {
	    dbAppendTable(DB_CON, output_table, xdf)
	}
	})
	if (.i %% update_every == 0) message(glue("  Finished {.i} ID's in {round(difftime(Sys.time(), start, units = 'mins'), 1)} minutes."))
    }
    message(glue("All done, in {round(difftime(Sys.time(), start, units = 'mins'), 1)} minutes."))
}
