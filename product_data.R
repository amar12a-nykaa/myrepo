require(RSiteCatalyst)
require(dplyr)
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
                           metrics =  c("event5", "cartadditions", "orders"),
                           elements = "product",
                           start = i,
                           top = 5000,
                           date.granularity = "year",
                           interval.seconds = 5,
                           max.attempts = 100,
                           validate = TRUE)
  raw_perm <- rbind(raw_perm, raw_temp)
  i <- i + 5000
  timestamp();
  print(ceiling(i/5000)-1);
  print(nrow(raw_temp));
  print(nrow(raw_perm));
}
website_product_data <- raw_perm
website_product_data[is.na(website_product_data)] <- 0



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
                           metrics =  c("event5", "cartadditions", "orders"),
                           elements = "product",
                           start = i,
                           top = 5000,
                           date.granularity = "year",
                           interval.seconds = 5,
                           max.attempts = 100,
                           validate = TRUE)
  raw_perm <- rbind(raw_perm, raw_temp)
  i <- i + 5000
  timestamp();
  print(ceiling(i/5000)-1);
  print(nrow(raw_temp));
  print(nrow(raw_perm));
  
}
app_product_data <- raw_perm



## Combining data
product_data <- rbind(website_product_data, app_product_data)
product_data <- aggregate(cbind(event5, cartadditions, orders)~name, data = product_data, sum)
product_data$cart_addition_percent <- product_data$cartadditions*100/product_data$event5
product_data$cart_success <- product_data$orders*100/product_data$cartadditions
product_data <- select(product_data, name, event5, cart_addition_percent, cart_success)
colnames(product_data)[1] <- 'product_id'
colnames(product_data)[2] <- 'product_views'
product_data <- arrange(product_data, desc(product_views))
