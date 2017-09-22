######################################################################
################ 원유 수불부 정리
######################################################################

##############################
############### Settings

##### Attaching Packages
library(readxl)
library(forcats)
library(stringr)
library(stringi)
library(plyr)
library(dplyr)
library(tidyr)
library(openxlsx)
library(lubridate)
library(purrr)
library(xts)
library(data.table)
library(tibble)
library(purrrlyr)
library(rebus)


##### Path 설정
path1 <- "P:/채승/Rwd/수불부/원유수불부"
path2 <- "P:/채승/Rwd/수불부/원유수불부/파일"


##############################
############### 데이터 불러오기

##### Setting Start -> End
start_y <- 13
start_m <- 1
end_y <- 17
end_m <- 7


##### Creating variables
month_num <- (end_y - start_y)*12 + (end_m - start_m) + 1
years <- (1:month_num - 1)/12 + 
  as.yearmon(paste("20", start_y, ifelse(str_length(start_m) == 2, "-", "-0"), 
                   start_m, "-01", sep = ""))
filenms <- paste0("CRDS", year(years) %>% str_sub(3, 4), ifelse(str_length(month(years)) == 1, "0", ""), month(years), ".xlsx", sep = "")
filenms_ <- paste0(year(years) %>% str_sub(3, 4), ifelse(str_length(month(years)) == 1, "0", ""), month(years), sep = "")


##### 시트명 확인
setwd(path2)

sheetnms_list <- filenms %>% map(~ excel_sheets(path = .)) 
sheetnms <- sheetnms_list %>% flatten %>% unlist %>% table %>% names()
sheetnms_list %>% map(~ sum(. == "원유처리현황")) %>% flatten %>% unlist


##### 데이터 불러오기
rawdata <- filenms %>% map(~ read_excel(path = ., sheet = "원유처리현황", col_names = TRUE, skip = 4, na = "NA"))
names(rawdata) <- paste0(year(years) %>% str_sub(3, 4), ifelse(str_length(month(years)) == 1, "0", ""), month(years), sep = "")


##############################
############### 데이터 Cleaning

##### NA 행 지우기
raw_clean_1 <- rawdata %>% map(function(df) {(is.na(df) %>% apply(., 1, sum) != length(df)) %>% df[., ]})


##### NA 열 지우기
raw_clean_2 <- raw_clean_1 %>% map(function(df) {(is.na(df) %>% apply(., 2, sum) != nrow(df)) %>% df[, .]})


##### 변수명 확인
colnms <- raw_clean_2 %>% map(~ names(.)) %>% flatten %>% unlist %>% table %>% names
colnms


##### 변수 제거 - BD 물량 부분
raw_clean_3 <- raw_clean_2 %>% map(function(df) {
  df$유종 <- str_detect(names(df), pattern = "월" %R% END) %>% which %>% df[[.]] 
  df <- df[, which(names(df) == "유종"):length(df)]
  })


##### NA 재검증
raw_clean_4 <- raw_clean_3 %>% map(function(df) {(is.na(df) %>% apply(., 1, sum) != length(df)) %>% df[., ]})
raw_clean_4 %>% map(~ is.na(.) %>% apply(1, sum))

raw_clean_5 <- raw_clean_4 %>% map(function(df) {(is.na(df[, -1]) %>% apply(., 1, sum) != length(df[, -1])) %>% df[., ]})
raw_clean_5 %>% map(~ is.na(.) %>% apply(1, sum)) %>% lapply(sum) %>% flatten %>% unlist


##### 변수명 바꾸기
colnms <- raw_clean_5 %>% map(~ names(.)) %>% flatten %>% unlist %>% table %>% names
colnms
colnms_change <- rbind(colnms, c("CDU1", "CDU2", "CDU1_A", "CDU1_B", "total", "crude_name", "total2"))

raw_clean_6 <- raw_clean_5 %>% map(function(df) {
  names(df) <- match(names(df), colnms_change[1, ]) %>% colnms_change[2, .]
  df
  })


##### 합계2 지우고, 단위 환산(리터 -> 배럴)
raw_clean_7 <- raw_clean_6 %>% map(function(df) {
  df <- df[, -which(names(df) == "total2")]
  df[, -which(names(df) == "crude_name")] <- df[, -which(names(df) == "crude_name")]/158.984
  df
})


##### merge names
raw_clean_7 %>% map(function(df) {(df$crude_name %>% table > 1) %>% table(df$crude_name)[.] %>% as.numeric}) %>% 
  flatten %>% unlist %>% sum

HS_crudenms <- raw_clean_7 %>% map(function(df) {1:which(df$crude_name == "HS TOTAL") %>% df$crude_name[.]}) %>% 
  flatten %>% unlist %>% table %>% names %>% walk(., assign, x = "HS_crudenms", value = ., envir = .GlobalEnv) %in% 
  c("WTR", "Slop Oil", "HS TOTAL", "CHA", "SLE", "DUR") %>% 
  (function(df) !df) %>% HS_crudenms[.] %>% c(., "WTR", "Slop Oil", "HS TOTAL")

LS_crudenms <- raw_clean_7 %>% map(function(df) {if (sum(str_detect(df$crude_name, "LS TOTAL")) == 0) {
  ""
  } else {
    (which(df$crude_name == "HS TOTAL") + 1):nrow(df) %>% df$crude_name[.]
  }
  }) %>% flatten %>% unlist %>% table %>% names %>% walk(., assign, x = "LS_crudenms", value = ., envir = .GlobalEnv) %in% 
  c("", "CHA", "DUR", "SLE", "Total", "TOTAL") %>%
  (function(df) !df) %>% LS_crudenms[.] %>% c("CHA", "DUR", "SLE", ., "Total", "TOTAL")

all_crudenms <- c(HS_crudenms, LS_crudenms) %>% 
  (function(df) {df[!(df %in% c("HS TOTAL", "LS TOTAL", "WTR", "Slop Oil", "Total", "TOTAL"))]}) %>%
  c(., "Slop Oil", "HS TOTAL", "LS TOTAL", "Total", "TOTAL", "WTR")
  
all_df <- data.frame(crude_name = all_crudenms)


##############################
############### 데이터 합치기

##### merge data
data_merge <- raw_clean_7 %>% 
  map2(.y = map(1:length(raw_clean_7), function(num) names(raw_clean_7)[num]), 
       function(df1, df2) {
         df1 <- left_join(all_df, df1, by = "crude_name") %>% (function(df1) {df1[is.na(df1)] <- 0; df1}) 
         df1$month <- df2
         df1[, c(1, length(df1), 2:(length(df1) - 1))]
         }) %>% bind_rows 

data_merge %>% View()


##############################
############### 최종 변환

##### final transforming
CDU1_data <- data_merge[, c("crude_name", "month", "CDU1")] %>% spread(key = month, value = CDU1) %>%
  (function(df) {df$crude_name <- factor(df$crude_name, levels = all_crudenms); df}) %>% 
  arrange(crude_name)

CDU2_data <- data_merge[, c("crude_name", "month", "CDU2")] %>% spread(key = month, value = CDU2) %>%
  (function(df) {df$crude_name <- factor(df$crude_name, levels = all_crudenms); df}) %>%
  arrange(crude_name)


##############################
############### 저장

setwd(path1)

rm(wb)
wb <- openxlsx::createWorkbook() 

addWorksheet(wb, "CDU1")
addWorksheet(wb, "CDU2")

writeData(wb, sheet = "CDU1", CDU1_data, startCol = 1, startRow = 1)
writeData(wb, sheet = "CDU2", CDU2_data, startCol = 1, startRow = 1)

openxlsx::saveWorkbook(wb, paste("원유수불(", 
                                 filenms_[1], "~", 
                                 filenms_[length(filenms_)],
                                 ").xlsx", sep = ""), overwrite = TRUE) 