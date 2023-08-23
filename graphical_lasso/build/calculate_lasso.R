# generate equity pairs lasso results and pushing to BQ

library(googleAuthR)
library(bigrquery)
library(tidyverse)
library(glue)
library(lubridate)
library(glasso)

# Settings ----------------------------------------------------------------

# cloud settings
PROJECT <- "your-gcp-project-id"
RETURNS_DATASET <- "your-bigquery-returns-dataset"     # read
RETURNS_TABLE <- "your-bigquery-returns-table"         # read
SECTORS_TABLE <- "your-bigquery-sectors-table"         # read
LASSO_DATASET <- "your-bigquery-lasso-feature-dataset" # write
LASSO_TABLE <- "your-bigquery-lasso-feature-table"     # write

# parameters
LOOKBACK <- 3  # years
RHO <- 0.3
MIN_RETURNS <- 500  # if a ticker has less than MIN_RETURNS returns, filter it

# Helper functions --------------------------------------------------------

# Get returns data
get_returns <- function(universe_year) {
  from_date <- universe_year - LOOKBACK
  tb <- bigrquery::bq_project_query(
    PROJECT,
    query = glue::glue(
      "SELECT *
    FROM `{RETURNS_DATASET}.{RETURNS_TABLE}`
    WHERE date >= '{from_date}-01-01';"
    )
  )
  
  if(as.numeric(bigrquery::bq_table_meta(tb)$numRows) > 0) {
    returns <- bigrquery::bq_table_download(tb)
  } else {
    returns <- NULL
  }  
  print(head(returns))
  returns
}

get_pw_sectors <- function(returns) {
  # get industry sectors
  tb <- bigrquery::bq_project_query(
    PROJECT,
    query = glue::glue(
      "SELECT *
    FROM `{RETURNS_DATASET}.{SECTORS_TABLE}`;"
    )
  )
  
  if(as.numeric(bigrquery::bq_table_meta(tb)$numRows) > 0) {
    sectors <- bigrquery::bq_table_download(tb)
  } else {
    sectors <- NULL
  }  
  print(head(sectors))
  
  # get issamesector, issameindustry variables
  sectors <- sectors %>% 
    select(ticker, sector, industry) %>% 
    filter(ticker %in% (returns %>% distinct(ticker) %>% pull())) %>% 
    na.omit() 
  
  pw_sectors <- sectors %>% 
    expand(ticker, ticker) %>% 
    rename_with(~str_remove_all(., "\\.")) %>% 
    filter(ticker1 != ticker2) %>% 
    # filter so we get only one combinatin of stock1-stock2
    mutate(tickers = ifelse(ticker1 < ticker2, glue("{ticker1}, {ticker2}"), glue("{ticker2}, {ticker1}"))) %>%
    distinct(tickers, .keep_all = TRUE) %>%
    select(-tickers) %>%
    left_join(sectors, by = c("ticker1"="ticker")) %>% 
    left_join(sectors, by = c("ticker2"="ticker"), suffix = c("1", "2")) 
  
  pw_sectors
}

# reduce universe
reduce_universe <- function(returns) {
  to_filter <- returns %>% 
    group_by(ticker) %>% 
    summarise(count = n()) %>% 
    filter(count < MIN_RETURNS) %>% 
    pull(ticker)
  
  returns %>% 
    filter(!ticker %in% to_filter)
}

# make covariance matrix
get_cov <- function(returns) {
  # make wide returns df
  wide_returns <- returns %>% 
    pivot_wider(date, names_from = ticker, values_from = daily_returns)
  
  # make covariance matrix on returns
  S <- wide_returns %>%
    select(-date) %>% 
    scale(center=TRUE, scale=TRUE) %>%
    cov(use='pairwise.complete.obs')

  # check symmetry
  if(!isSymmetric(S)) {
    S[lower.tri(S)] = t(S)[lower.tri(S)]  
  }
  print(S[1:10, 1:10])
  S
}

# estimate precision matrix using glasso
get_pcorrs <- function(S, pw_sectors) {
  # sectors to zero
  to_zero <- pw_sectors %>%
    filter(sector1 != sector2 | industry1 != industry2) %>% 
    select(ticker1, ticker2) %>% 
    as.matrix(byrow = TRUE)
  
  idxs_to_zero <- cbind(
    match(to_zero[, 1], rownames(S)),  # row indexes of S that match ticker1
    match(to_zero[, 2], rownames(S))  # row indexes of S that match ticker2 (rows and columns are ordered the same)
  ) 
  
  # Some elements will have NA - where two tickers didn't share any data. Set those to zero and constrain to zero in Lasso.
  missing <- which(is.na(S) | is.nan(S) | is.infinite(S), arr.ind = TRUE) # arr.ind returns matrix of row,col elements - just what glasso needs for its zero parameter
  
  dimnames(missing) <- NULL
  
  # combined matrix of indexes to zero in precision matrix
  idxs_to_zero <- rbind(idxs_to_zero, missing)
  
  # convert missing values to zero - glasso will error otherwise
  S[is.na(S) | is.nan(S) | is.infinite(S)] <- 0
  
  print(glue("Attempting graphical lasso on covariance matrix of dimensions {dim(S)}"))
  print(Sys.time())
  # note Meinhausen-Buhlman approximation is ok for our purposes: 
  # estimates the set of non-zero elements rather than their actual partial correlations
  # https://arxiv.org/pdf/0708.3517.pdf
  invcov <- glasso(S, rho = RHO, zero = idxs_to_zero, approx = TRUE, trace = TRUE)  
  print(glue("Done at {Sys.time()}"))
  
  # extract precision matrix
  P <- invcov$wi
  colnames(P) <- colnames(S)
  rownames(P) <- colnames(P)
  
  # check symmetry
  if(!isSymmetric(P)) {
    P[lower.tri(P)] = t(P)[lower.tri(P)]  
  }
  
  P
}

# get lasso feature table
make_lasso_feature <- function(P, universe_year) {
  P %>% 
    as.data.frame() %>% 
    tibble::rownames_to_column("stock1") %>% 
    pivot_longer(-stock1, names_to = "stock2", values_to = "pcorr") %>% 
    # filter diagonal
    filter(stock1 != stock2) %>% 
    # filter so we get only one combinatin of stock1-stock2
    mutate(tickers = ifelse(stock1 < stock2, glue("{stock1}, {stock2}"), glue("{stock2}, {stock1}"))) %>%
    distinct(tickers, .keep_all = TRUE) %>%
    select(-tickers) %>% 
    # lasso feature
    mutate(isLassoApproved = case_when(pcorr == 0 ~ FALSE, TRUE ~ TRUE)) %>% 
    select(-pcorr) %>% 
    mutate(universeyear = universe_year)
}

calculate_lasso <- function(universe_year) {
  print(glue("Starting process at {Sys.time()}"))
  
  universe_year <- as.numeric(universe_year)
  print(glue("Doing lasso feature for {universe_year}"))
  
  print(glue("Getting returns"))
  returns <- get_returns(universe_year)
  
  print(glue("Filtering universe on {MIN_RETURNS} data points"))
  returns <- reduce_universe(returns)
  
  print("Getting pairwise sectors")
  pw_sectors <- get_pw_sectors(returns)
  
  print(glue("Calculating covariances"))
  S <- get_cov(returns)
  rm(returns)
  
  print(glue("Calculating partial correlations"))
  P <- get_pcorrs(S, pw_sectors)
  rm(S, pw_sectors)
  
  print(glue("Making feature"))
  feature <- make_lasso_feature(P, universe_year)
  num_approved_pairs <- feature %>% group_by(isLassoApproved) %>% summarise(count = n())
  print("Number of graphical lasso approved pairs")
  print(num_approved_pairs)
  
  # Write weights to BQ 
  print(glue("Writing to BQ"))
  # delete if data exists
  dataset <- bq_dataset(PROJECT, LASSO_DATASET)
  tb <- bq_dataset_query(
    x = dataset,
    query = glue("DELETE FROM {LASSO_TABLE} WHERE universeyear={universe_year};")
  )
  
  # write back to BQ table 
  bq_table <- bq_table(project = PROJECT, dataset = LASSO_DATASET, table = LASSO_TABLE)
  tb <- bq_table_upload(
    # note: wraps bigrquery::bq_perform_upload
    x = bq_table, 
    values = (feature %>% filter(isLassoApproved == TRUE)), 
    create_disposition = 'CREATE_NEVER',  # return error if table does not exist
    write_disposition = 'WRITE_APPEND'    # append if table exists
  )
  
  print(glue("Process done at {Sys.time()}"))
  return("200")
}

calculate_lasso(2022)