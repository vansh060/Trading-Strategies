library(arrow)
library(data.table)
library(tidyverse)
library(dplyr)
library(lubridate)
library(bizdays)
library(zoo)

# loading india all tickers data (2000-2024) of 120 variables
# file is in a parquet format of 4 gb with more than 20 million rows of data
india_full_data <- read_parquet('Downloads/India (1).parquet')
india_full_data <- india_full_data %>% select(RIC, date, ret,ret_M)
india_full_data = india_full_data %>% mutate(date1 = date)
setDT(india_full_data)

# loading india dividends data from 2000-2024 
dividends = read_csv('Downloads/final_India_cash_dividends(monthly_dividend_testing).csv')
setDT(dividends)
setnames(dividends, "Ticker", "RIC")


# creating function which includes data processing for 'dividends' based on the analysis period 
# and combining dividends data with 'india_full_data'
combining_datasets = function(X, Y, before_exdiv) {
  # Convert before_exdiv to vector matching length of dividends
  dividends = dividends %>% 
    filter(Dividend_Type %in% "Final_Cash Dividend") %>%
    mutate(
      before_exdiv_vec = rep(before_exdiv, n()),
      extended_start = case_when(
        before_exdiv_vec ~ as.Date(Announcement_Date, format = "%d/%m/%y") + X,
        !before_exdiv_vec ~ as.Date(ExDividend_Date, format = "%d/%m/%y") + X
      ),
      extended_end = case_when(
        before_exdiv_vec ~ as.Date(ExDividend_Date, format = "%d/%m/%y") - Y,
        !before_exdiv_vec ~ as.Date(ExDividend_Date, format = "%d/%m/%y") + Y
      ),
      div_anndate = as.Date(Announcement_Date, format = "%d/%m/%y"),
      div_exdate = as.Date(ExDividend_Date, format = "%d/%m/%y")
    ) %>%
    filter(extended_start <= extended_end) %>%
    unique() %>%
    setDT()
  
  setkey(india_full_data, RIC, date, date1)
  setkey(dividends, RIC, extended_start, extended_end)
  
  final_data <- foverlaps(india_full_data, dividends,
                          by.x = c("RIC", "date", "date1"),
                          by.y = c("RIC", "extended_start", "extended_end"),
                          type = "within",
                          nomatch = 0L) %>% 
    mutate(days_tilExdate = date - div_exdate)
  
}

# this visualisation primarily helps in understadning excess returns over market returns and thus building 
# strategy across different X and Y values primarily by taking long position for some duration before ex-dividend date and 
# take short position after ex-dividends for some duration. 

excess_retruns_visualisation <- function(time_period, final_data){
  visualisation = final_data %>% filter(days_tilExdate >= time_period) %>% 
    group_by(days_tilExdate) %>% 
    summarise(N = n(),
              mu = mean(ret - ret_M),
              sd = sd((ret-ret_M)),
              se = sd/sqrt(n()));
  ggplot(visualisation) + geom_point(aes(x = days_tilExdate, y = mu)) +
    geom_errorbar(aes(x = days_tilExdate, ymin = mu - se, ymax = mu + se)) +
    geom_line(aes(x = days_tilExdate, y = mu)) +
    geom_hline(yintercept = 0, linetype = "dashed", col = "blue") +
    geom_vline(xintercept = 0, linetype = "dashed", col = "red") +
    geom_vline(xintercept = -15, linetype = "dashed", col = "red", alpha = 0.5) +
    geom_vline(xintercept = 7, linetype = "dashed", col = "red", alpha = 0.5) +
    # scale_y_continuous(limits = c(-1,2)) +
    labs(title = "Avg Ret by Days before Ex Date",
         subtitle = "Starting 5 cal. days after Ann Date and ending 1 cal. days before ex date \n
         Clipping only those with at least 30 days before ex date but after ann date though.",
         y = "Avg Ret", x = "Days until Ex-Date");
}

# looking for alpha across different holding periods 
alpha_testing = function(final_data, x , y){
  
  grid_month <- india_full_data %>% select(date,ret_M) %>% unique %>% 
    mutate(date = as.Date(as.yearmon(date))) %>% 
    group_by(date) %>% summarise(ret_M = (prod(1+coalesce(ret_M)/100)-1)*100) %>% ungroup
  
  port <- final_data %>%
    filter(days_tilExdate > x & days_tilExdate < y) %>%
    filter(year(date) >= 2000) %>%
    mutate(rand = runif(n())) %>% as_tibble %>%
    group_by(date) %>% 
    # slice_max(order_by = rand, n = 30) %>% select(-rand) %>%
    summarise(N = n(),
              ret = mean((ret - ret_M))) %>%
    ungroup %>%
    mutate(date = as.Date(as.yearmon(date))) %>% 
    group_by(date) %>%
    summarise(N = mean(N),
              ret = (prod(1+ret/100)-1)*100) %>% 
    merge(x = ., y = grid_month, by = "date", all.y = T)
  
  cat("\nRegression with all data:\n")
  print(summary(lm(ret ~ ret_M, data = port)))
  
  cat("\nRegression with N > 5:\n")
  print(summary(lm(ret ~ ret_M, data = port %>% filter(N>5))))
  
  cat("\nRegression with N > 10:\n")
  print(summary(lm(ret ~ ret_M, data = port %>% filter(N>10))))
  
  cat("\nRegression with N > 20:\n")
  print(summary(lm(ret ~ ret_M, data = port %>% filter(N>20))))
  
}

# We need to change X and Y values according to the analysis 
final_data1 <- combining_datasets(X =1, Y = 5, before_exdiv = FALSE)
excess_retruns_visualisation(time_period = -30,final_data = final_data1)
alpha_testing(final_data = final_data1, x = 1, y = 5)

