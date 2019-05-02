# install.packages("remotes")
remotes::install_cran(c("sparklyr", "tidyverse"))


library(dplyr)
library(sparklyr)

# make sure spark is installed
spark_install()


# set up local connection
sc <- spark_connect(master = "local")

# get data
remotes::install_cran(c("nycflights13", "Lahman"))


# copy data into spark
iris_tbl <- copy_to(sc, iris)
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")

# look at src tables
src_tbls(sc)

# display top 10 flights with a delay of 2 minutes
flights_tbl %>%
  filter(dep_delay == 2)


# collect all unique tail numbers: count, avg distance, and avg delay
# with count more than 10 and dist less than 2000
delay <-
  flights_tbl %>%
  ... %>%
  collect()

# display all unique delay information
delay

# plot delay ~ dist with size as count
# with a smoother
library(ggplot2)
ggplot(delay, aes(....)) +
  geom_point(alpha = 1/2) +
  .... +
  scale_size_area(max_size = 2)
