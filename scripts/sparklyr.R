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
flights_tbl %>% filter(dep_delay == 2)


# collect all unique tail numbers: count, avg distance, and avg delay
# with count more than 10 and dist less than 2000
delay <-
  flights_tbl %>%
  group_by(tailnum) %>%
  summarise(
    count = n(),
    dist = mean(distance, na.rm = TRUE),
    delay = mean(dep_delay, na.rm = TRUE)) %>%
  filter(
    count > 10,
    dist < 2000,
    !is.na(delay)
  ) %>%
  collect()

# display all unique delay information
delay

# plot delay ~ dist with size as count
# with a smoother
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)





# get all flight delays, update month and day, select certain columns
df <- flights_tbl %>%
  filter(
    distance < 2000,
    !is.na(dep_delay)
  ) %>%
  mutate(
    month = paste0("m", month),
    day = paste0("d", day)
  ) %>%
  select(dep_delay, sched_dep_time, month, day, distance)



# # look at sql code being generated for execution
# ft_dplyr_transformer(sc, df) %>%
#   ml_param("statement") %>%
#   cat()


# The following step will create a 5 stage pipeline:
#
# 1. SQL transformer - Resulting from the ft_dplyr_transformer() transformation
# 2. Binarizer - To determine if the flight should be considered delay. The eventual outcome variable.
# 3. Bucketizer - To split the day into specific hour buckets
# 4. R Formula - To define the model’s formula
# 5. Logistic Model
flights_pipeline <-
  ml_pipeline(
    sc) %>%
  ft_dplyr_transformer(
    tbl = df) %>%
  ft_binarizer(
    input_col = "dep_delay",
    output_col = "delayed",
    threshold = 15) %>%
  ft_bucketizer(
    input_col = "sched_dep_time",
    output_col = "hours",
    splits = c(400, 800, 1200, 1600, 2000, 2400)) %>%
  ft_r_formula(
    delayed ~ month + day + hours + distance) %>%
  ml_logistic_regression()


flights_pipeline



# Partition the data into training and testing
partitioned_flights <-
  sdf_partition(
    flights_tbl,
    training = 0.1,
    testing = 0.1,
    rest = 0.8)

# Get Predictions from model fit
predictions <-
  flights_pipeline %>%
  ml_fit(partitioned_flights$training)
  ml_transform(partitioned_flights$testing)
predictions


prediction_counts <-
  predictions %>%
  group_by(delayed, prediction) %>%
  tally() %>%
  collect() %>%
  mutate(
    perc = round(n / sum(n) * 100)
  ) %>%
  merge(
    tribble(
      ~correct, ~delayed, ~prediction,
      TRUE, 0, 0,
      TRUE, 1, 1,
      FALSE, 1, 0,
      FALSE, 0, 1
    )
  ) %>%
  arrange(desc(correct), desc(n))
prediction_counts

# Area under ROC
ml_binary_classification_evaluator(predictions)
# RMSE
ml_regression_evaluator(predictions)
