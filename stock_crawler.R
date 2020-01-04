rm(list = ls())
gc()
library(data.table)
library(jsonlite)
library(RSQLite)
library(magrittr)

# function ========
fac_to_num = function(input) {
  return(as.numeric(gsub('[$,]', '', as.character(input))))
}

# connect db ========
con = dbConnect(RSQLite::SQLite(), dbname = 'stock.sqlite3')

# config========
st_day = 1
diff_day = 2
qtype = 'ALL' #全部

for (diff_day_n in st_day:(st_day + diff_day))
{
  date_in_int = as.integer(format(Sys.Date() - diff_day_n, "%Y%m%d"))
  print(date_in_int)
  
  print('---------- Check Data of Date is_insert -----------')
  check_date = dbGetQuery(
    con,
    paste(
      "select is_insert from data_of_date where date = ",
      date_in_int,
      sep = ""
    )
  )
  if (nrow(check_date) == 0)
  {
    print('---------- insert data_of_date -----------')
    sendquery =
      dbSendQuery(
        con,
        paste(
          "insert into data_of_date (date,is_insert) values (",
          date_in_int,
          ",0)",
          sep = ""
        )
      )
    
    dbHasCompleted(sendquery)
    dbClearResult(sendquery)
    check_date = dbGetQuery(con,
                             paste(
                               "select is_insert from data_of_date where date = ",
                               date_in_int,
                               sep = ""
                             ))
  }
  if (check_date$is_insert == 1)
  {
    # is_insert = 1表示DB已有資料，則跳過這天資料
    print('---------- is_insert = 1 -----------')
    next
  }
  print('---------- insert data -----------')
  
  
  url = paste0(
    'http://www.twse.com.tw/exchangeReport/MI_INDEX?',
    'response=json&date=',
    date_in_int ,
    '&type=',
    qtype
  )
  
  x_flag = T
  while(x_flag){
    tryCatch({
      # 有爬到資料則改x_flag為F跳脫while
      url_data = fromJSON(url)
      x_flag = F
    },
    error  = function(msg) {
      # 被ban ip後20分鐘再啟動
      message("Original warning message:\n")
      message(paste0(msg,"\n"))
      message(paste0('Now Sleep.',"\n"))
      Sys.sleep(120)
      message(paste0('Now Wake Up.',"\n"))
    }
    )
  }

  # stock_all 所有股票 ==============
  stock_all = data.table(url_data$data9)
  if (nrow(stock_all) == 0) {
    # 假日空資料跳下一圈
    next
  }
  stock_all$V10 = NULL
  colnames(stock_all) = c('stockId',
                          'stockName',
                          'tradeStock',
                          'tradeCount',
                          'tradeAmount',
                          'openValue',
                          'maxValue',
                          'minValue',
                          'closeValue',
                          'diffValue',
                          'lastBuyValue',
                          'lastBuyStock',
                          'lastSellValue',
                          'lastSellStock',
                          'PER'
                          )
  stock_all_char = apply(stock_all[,1:2], 2, as.character) %>% data.table()
  stock_all_num = apply(stock_all[,3:(ncol(stock_all)-2)], 2, fac_to_num) %>% data.table()
  stock_all = cbind(stock_all_char, stock_all_num) %>%
    cbind(stock_all, "Date" = as.character(Sys.Date() - diff_day_n))

  dbWriteTable(con, "main_data", stock_all, append = T)
  

  # index 指數============================
  index_data = data.frame(url_data$data1)
  index_data$X3 = NULL
  index_data$X5 = NULL
  colnames(index_data) = c('stockIndex','closeValue','diffValue')

  index_data$stockIndex  = as.character(index_data$stockIndex)
  index_data$closeValue  = fac_to_num(index_data$closeValue)
  index_data$diffValue  = fac_to_num(index_data$diffValue)
  index_data = cbind(index_data, "Date" = as.character(Sys.Date() - diff_day_n))
  dbWriteTable(con, "index_data", index_data, append = T)
  
  #str(index_data)
  sendquery =
    dbSendQuery(con,
                paste(
                  "update data_of_date set is_insert = 1 where date = ",
                  date_in_int,
                  sep = ""
                ))
  
  dbHasCompleted(sendquery)
  dbClearResult(sendquery)
  print('---------- insert success -----------')
  print('---------- Sleep -----------')
  Sys.sleep(20 + sample(5)[1])
  print('---------- Wake up -----------')
  
}




dbDisconnect(con)
