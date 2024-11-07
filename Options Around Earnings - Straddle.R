library(arrow)
library(data.table)
library(tidyverse)
library(dplyr)
library(lubridate)
library(bizdays)

tickers_global <- read_csv("/Users/vanshgupta/Downloads/tickers_full_appended_v2(active+dead) (1).csv")
tickers <- tickers_global %>% filter(Market %in% 'United States') %>% select(RIC, ticker)

# Create financial calendar 
holidays <- as.Date(c("2000-01-01", "2024-12-25")) # Define holidays
create.calendar(name = "MyCalendar", weekdays = c("saturday", "sunday"), holidays = holidays, adjust.from = "next")

# Key Inputs - Change this according to the different situation 
# With below input - we hold straddle for 15 days beofre earnings announcement and the first option pair with the expiration of more than 21 days
start_date_difference = -18
end_date_difference = -3
option_expiration_length = 21

earnings = read_csv("/Users/vanshgupta/Downloads/final_earning_data.csv")
earnings1 <- earnings %>% 
  left_join(tickers, by = 'RIC') %>%
  filter(is.na(ticker) == FALSE) %>%
  distinct(Event_Description, .keep_all = TRUE) %>%
  filter(Market == 'United States') %>%
  mutate(
    year_month = format(Event_Date, "%Y-%m"),
    common = paste(RIC, Event_Date, sep = '_'),
    sdate = as.Date(add.bizdays(Event_Date, start_date_difference, "MyCalendar")),
    edate = as.Date(add.bizdays(Event_Date, end_date_difference, "MyCalendar"))
  ) %>%
  filter(edate >= sdate, Event_Date <= as.Date('2023-12-31')) %>%
  setDT() %>%
  select(Event_Date, sdate, edate, everything()) %>%
  filter(Event_Date > as.Date('2021-01-01'))

# Function to get 4 nearest call and put options to the underlying asset's price 
# Objective is to build straddle which includeds taking entry and exit based on the strategy  
closest_option_wrt_assetprice <- function(option_type, price_comparison, slice_position) {
  set %>%
    mutate(sdate = as.Date(sdate),ExpirationDate = as.Date(ExpirationDate)) %>%
    filter(as.numeric(difftime(ExpirationDate, sdate, units = "days")) > option_expiration_length) %>%
    group_by(RIC) %>%
    arrange(ExpirationDate, StrikePrice) %>%
    filter(ExpirationDate == first(ExpirationDate)) %>%
    ungroup() %>%
    group_by(RIC, ExpirationDate) %>%
    filter(PutCall == option_type & eval(parse(text = price_comparison))) %>%
    slice(slice_position)
}

# Obtain final straddle dataframes with entry and exit price for all put and call otpions across different events
folder_path <- "/Users/vanshgupta/Downloads/Options Parquet Files/"
options_parquet_files <- list.files(folder_path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
df <- data.frame()
final_straddle_entry_exit <- data.frame()
options_parquet_files

for (i in 1:length(options_parquet_files)){
  print(i)
  data <- read_parquet(options_parquet_files[i]) %>% mutate(DataDate = as.Date(DataDate), ExpirationDate = as.Date(ExpirationDate))
  set <- left_join(earnings1, data, by = c('ticker' = 'Symbol', 'sdate'='DataDate')) %>%
    filter(is.na(ExpirationDate) == FALSE)
  
  # Apply function for each check
  case1 <- closest_option_wrt_assetprice("put", "UnderlyingPrice < StrikePrice", 1)
  case2 <- closest_option_wrt_assetprice("put", "UnderlyingPrice > StrikePrice", n())
  case3 <- closest_option_wrt_assetprice("call", "UnderlyingPrice > StrikePrice", n())
  case4 <- closest_option_wrt_assetprice("call", "UnderlyingPrice < StrikePrice", 1)
  
  all_cases <- bind_rows(case1,case2,case3,case4) 
  
  closest_strikeprice_option_pair <- all_cases %>%
    mutate(distance = abs(UnderlyingPrice - StrikePrice)) %>% group_by(RIC,PutCall) %>% 
    filter(distance == min(distance))
  
  df <- bind_rows(df,closest_strikeprice_option_pair)
  
  exit_price_merge <- df %>%
    left_join(data, by = c('edate' = 'DataDate','ticker' = 'Symbol','PutCall','ExpirationDate','StrikePrice'))
  
  exit_closest_strikeprice_option_pair <- exit_price_merge %>% 
    filter(is.na(UnderlyingPrice.y) == FALSE)
  
  final_straddle_entry_exit <- bind_rows(final_straddle_entry_exit, exit_closest_strikeprice_option_pair)
}

# Return calculation 
# We took open interest (the number of lots pending at the end of the day) greater than 10 to avoid options that are not liquid (as mentioned in academic papers).
# The objective of taking return < 2000 is to remove infinte value which could be due to data issue 
testing <- final_straddle_entry_exit %>% arrange(Event_Date,RIC) %>% filter(OpenInterest.x > 10 & OpenInterest.y > 10) %>%
  mutate(return = LastPrice.y/LastPrice.x - 1) %>% filter(return<2000)
mean(testing$return)


  





