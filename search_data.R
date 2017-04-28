require(RSiteCatalyst)

SCAuth("soumen.seth:FSN E-Commerce","770f388b78d019017d5e8bd7a63883fb")

#Website Data
i <- 1
raw_perm <- data.frame()
raw_temp <- data.frame(1)
nrow(raw_temp)
while (nrow(raw_temp) != 0){
  raw_temp <- data.frame()
  raw_temp <- QueueTrended("fsnecommerceprod",
                           '2016-02-01',
                           Sys.Date() - 1,
                           metrics =  'instances',
                           elements = "evar6",
                           start = i,
                           top = 50000,
                           date.granularity = "year",
                           interval.seconds = 5,
                           max.attempts = 100,
                           validate = TRUE)
  raw_perm <- rbind(raw_perm, raw_temp)
  i <- i + 50000
  timestamp();
  print(ceiling(i/50000)-1);
  print(nrow(raw_temp));
  print(nrow(raw_perm));
}
website_search_data <- raw_perm


#App Data
i <- 1
raw_perm <- data.frame()
raw_temp <- data.frame(1)
nrow(raw_temp)
while (nrow(raw_temp) != 0){
  raw_temp <- data.frame()
  raw_temp <- QueueTrended("fsnecommercemobileappprod",
                           '2016-02-01',
                           Sys.Date() - 1,
                           metrics =  'instances',
                           elements = "evar6",
                           start = i,
                           top = 50000,
                           date.granularity = "year",
                           interval.seconds = 5,
                           max.attempts = 100,
                           validate = TRUE)
  raw_perm <- rbind(raw_perm, raw_temp)
  i <- i + 50000
  timestamp();
  print(ceiling(i/50000)-1);
  print(nrow(raw_temp));
  print(nrow(raw_perm));
  
}
app_search_data <- raw_perm
