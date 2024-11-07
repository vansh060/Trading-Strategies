library(arrow)
library(data.table)
library(tidyverse)
library(dplyr)
library(lubridate)
library(bizdays)
library(zoo)

# create calendar to remove holidays 
holidays <- as.Date(c("2000-01-01", "2024-12-25")) # Add your holidays here
create.calendar(name = "MyCalendar", weekdays = c("saturday", "sunday"), holidays
                = holidays, adjust.from = "next")

#winsorization function
w <- function(x, fraction = 0.01) {
  n <- length(x)
  lo <- floor(n * fraction) + 1
  hi <- n - lo + 1
  x[x < x[lo]] <- x[lo]
  x[x > x[hi]] <- x[hi]
  return(x)
}

# data preprocessing for segmenting combefore testing for segmenting and grouping 
# companies into decile with different SUE size. The focus is only for India market, but this same 
# strategy is applicable in other markets also as tested in the past. Since we need to limit the 
# of data, hence it is not included 

earnings <- read_csv("/Users/vanshgupta/Downloads/final_earning_data.csv")
earnings <- earnings %>%
  distinct(Event_Description, .keep_all = TRUE) %>%
  filter(Market == 'India') %>%
  mutate(
    year_month = format(Event_Date, "%Y-%m")
  ) %>%
  mutate(year_ear = year(Event_Date), month_ear = month(Event_Date)) %>%
  mutate(year_month_ear = paste(year_ear,month_ear,sep = "_")) %>%
  group_by(year_month) %>% 
  mutate(common = paste(RIC, Event_Date, sep = '_')) %>%
  ungroup() %>%
  mutate(
    sdate = add.bizdays(as.Date(Event_Date), 3, "MyCalendar"),
    edate = add.bizdays(as.Date(sdate), 200, "MyCalendar"),
    sdate = as.Date(sdate),
    edate = as.Date(edate)
  ) %>%
  filter(edate >= sdate) %>%
  distinct() %>%
  mutate(Event_Date = as.Date(Event_Date))%>%
  filter(Event_Date <= as.Date('2023-12-31')) %>%
  setDT() %>% mutate(quarter = floor_date(Event_Date, unit = 'quarter')) %>%
  arrange(quarter) %>%
  group_by(quarter) %>% filter(n()>=10) 

final_earnings_data <- data.frame()
for (i in seq(0.1, 1, by = 0.1)) {
  case1 <- earnings %>%
    group_by(quarter) %>%
    mutate(sue_comparison = quantile(SUE, i)) %>%
    mutate(quarter1 = quarter + months(3)) %>%
    ungroup() %>%
    select(quarter1, sue_comparison) %>%
    unique()
  
  case2 <- earnings %>%
    left_join(case1, by = c('quarter' = 'quarter1')) %>%
    filter(SUE <= sue_comparison)
  case3 <- case2 %>% mutate(group = i)
  if (i == 0.1) {
    final_earnings_data <- case3
  } else {
    #final1 <- anti_join(final, div12, by = c('Event_Description')) # Adjust'Event_Description' as needed
    final_earnings_data <- bind_rows(final_earnings_data, case3) 
  }
}
final_earnings_data <- final_earnings_data %>% group_by(Event_Description) %>% slice(1) %>% ungroup()

# join the initial earnings data with stock price data and winsorizing the returns 
india_market <- read_parquet('Downloads/India (1).parquet') 
india_market <- india_market %>% select(RIC, TR.PriceClose.CalcDate, date, ret, ret_M)

setDT(final_earnings_data)
setDT(india_market)
setkey(final_earnings_data, RIC, sdate, edate)
setkey(india_market, RIC, TR.PriceClose.CalcDate, date)
india_market <- india_market[!is.na(TR.PriceClose.CalcDate) & !is.na(date)]

final_data <- foverlaps(india_market, final_earnings_data,
                   by.x = c('RIC', 'TR.PriceClose.CalcDate', 'date'),
                   by.y = c('RIC', 'sdate', 'edate'),
                   type = 'within',
                   nomatch = 0L) %>%
  mutate(days_bw_pead = edate - date) %>%
  group_by(common) %>%
  mutate(days_bw_pead = row_number()) %>% ungroup() %>%
  arrange(ret) %>%
  mutate(return_winsorized = w(ret))



# testing for monthly excess returns for different SUE - primary focus is on firms with SUE 
# at the bottom and at the top, where we short bottom one and long top ones 
alpha_df <- data.frame()
sue <- sort(unique(final$group))

for (i in sue){
  final1 <- final_data %>% filter(group == i)
  print(paste(i))
  grid_month1 <- final1 %>%
      filter(days_bw_pead <= 60) %>%
      select(date, ret_M) %>%
      unique %>%
      arrange(date) %>%
      mutate(date = as.Date(as.yearmon(date)))%>%
      group_by(date) %>%
      summarise(ret_M = (prod(1 + coalesce(ret_M, 0) / 100) - 1) * 100) %>%
      ungroup()
  port <- final1 %>%
      filter(days_bw_pead <= 60) %>%
      mutate(rand = runif(n())) %>%
      as_tibble() %>%
      group_by(date) %>%
      summarise(N = n(),
                return_winsorized= mean(return_winsorized - ret_M)) %>%
      ungroup() %>%
      mutate(date = as.Date(as.yearmon(date))) %>%
      group_by(date) %>%
      summarise(N = mean(N),
                return_winsorized= (prod(1 + return_winsorized/ 100) - 1) * 100,
                countn = n()) %>%
      merge(x = ., y = grid_month1, by = "date", all.y = TRUE) %>%
      mutate(return_winsorized = coalesce(return_winsorized, 0)) %>%
      filter(year(date)>=2000)
  
    if (nrow(port) > 0) {
      reg1 <- lm(return_winsorized ~ ret_M, data = port)
      reg2 <- summary(reg1)
      rse <- reg2$sigma
      coefficients <- as.data.frame(t(coef(reg1)))
      coefficients$group <- i
      coefficients$months <- nrow(port)
      coefficients$rse <- rse
      coefficients$yearly_sharpe <- ((coefficients$`(Intercept)` / coefficients$rse) *
                                       sqrt(12))
      alpha_df <- bind_rows(alpha_df, coefficients)
    } else {
      print(paste("No non-NA data for group", i, "and market", j))
    }
}

# testing for daialy excess returns for different SUE - primary focus is on firms with SUE 
# at the bottom and at the top, where we short bottom one and long top ones 
final2 <- final_data %>% filter(group %in% 1) %>%
  filter(days_bw_pead <= 60) %>%
  mutate(ret = ret) %>%
  group_by(days_bw_pead) %>%
  summarise(N = n(),
            firm_ret = mean(ret, na.rm = TRUE),
            mkt_ret = mean(ret_M, na.rm = TRUE))
reg1 <- lm(firm_ret ~ mkt_ret, data = final2)
summary(reg1)

# Market wise strategy cumulative returns wrt market
final1 <- final_data %>% filter(group == 1) #group == 0.1 for bottom 10% and group == 1 for top 10%
check1 = final1 %>%
  group_by(Market,days_bw_pead) %>%
  summarise(N = n(),
            mu = mean(return_winsorized),
            mkt_ret = mean(ret_M)) %>%
  filter(days_bw_pead <= 60)
check2 <- check1 %>%
  mutate(cum_ret_M = cumprod(1 + mkt_ret / 100),
         cum_ret = cumprod(1 + mu / 100))
graph <- ggplot(check2) +
  geom_line(aes(x = days_bw_pead, y = cum_ret, color = "Top 10% SUE Firms")) +
  geom_line(aes(x = days_bw_pead, y = cum_ret_M, color = "Market Cumulative
Return")) +
  labs(title = "Cumulative Returns Over Time - Long in the Top Quintile",
       x = "Days Between PEAD",
       y = "Cumulative Returns in %",
       color = "Legend") +
  scale_color_manual(values = c("Top 10% SUE Firms" = "blue",
                                "Market Cumulative Return" = "red")) +
  facet_wrap(~ Market, scales = "free_y") +
  theme_minimal() +
  theme(
    legend.position = "top",
    legend.title = element_text(size = 8),
    legend.text = element_text(size = 6),
    legend.key = element_rect(fill = "white", color = "black"),
    legend.background = element_rect(fill = "white", color = "black"),
    strip.background = element_blank(),
    strip.text = element_text(size = 10),
    axis.title.x = element_text(size = 10),
    axis.text.x = element_text(size = 8),
    panel.grid.major = element_blank(), # Remove major grid lines
    panel.grid.minor = element_blank() # Remove minor grid lines
  )
  print(graph)
  
  
  
