library(reactlog)
options(shiny.reactlog = TRUE)



library(sparklyr)
library(shiny)
library(dplyr)
library(ggplot2)


ui <- fluidPage(

  titlePanel("Sparklyr demo"),

  sidebarLayout(
    sidebarPanel(
      sliderInput("count", "Min count:", 10, 400, value = 10),
      sliderInput("dist", "Distance:", 0, 2000, value = c(0,2000)),
      sliderInput("threshold", "Delay threshold:", 1, 40, value = 15),
      sliderInput("training", "Testing Percentage:", 1, 10, value = 2),
      sliderInput("testing", "Training Percentage:", 1, 10, value = 2)
    ),
    mainPanel(
      plotOutput("plot"),
      div("Model predictions: ", tableOutput("table")),
      div("AOC: ", verbatimTextOutput("aoc")),
      div("RMSE: ", verbatimTextOutput("rmse"))
    )
  )
)

sc <- spark_connect(master = "local")

# copy data in
if (!exists("flights_tbl")) {
  flights_tbl <- copy_to(sc, nycflights13::flights, "flights", overwrite = TRUE)
}

server <- function(input, output) {

  delay_data <- reactive({
    # get all unique tail numbers count, avg distance, and delay
    delay <-
      flights_tbl %>%
      group_by(tailnum) %>%
      summarise(
        count = n(),
        dist = mean(distance, na.rm = TRUE),
        delay = mean(dep_delay, na.rm = TRUE)) %>%
      filter(
        count > input$count,
        dist <= input$dist[2],
        dist >= input$dist[1],
        !is.na(delay)
      ) %>%
      collect()

    # display all unique delay information
    delay

  })
  output$plot <- renderPlot({
    # plot delays
    delay_data() %>%
      ggplot(aes(dist, delay)) +
        geom_point(aes(size = count), alpha = 1/2) +
        geom_smooth() +
        scale_size_area(max_size = 2)
  })



  # get all flight delays, update month and day, select certain columns
  flights_time <- flights_tbl %>%
    filter(!is.na(dep_delay)) %>%
    mutate(
      month = paste0("m", month),
      day = paste0("d", day)
    ) %>%
    select(dep_delay, sched_dep_time, month, day, distance)

  flights_df <- reactive({
    flights_time %>%
      filter(
        distance <= input$dist[2],
        distance >= input$dist[1]
      )
  })


  # The following step will create a 5 stage pipeline:
  #
  # 1. SQL transformer - Resulting from the ft_dplyr_transformer() transformation
  # 2. Binarizer - To determine if the flight should be considered delay. The eventual outcome variable.
  # 3. Bucketizer - To split the day into specific hour buckets
  # 4. R Formula - To define the model’s formula
  # 5. Logistic Model
  reactive_flights_pipeline <- reactive({
    df <- flights_df()
    sc %>%
      ml_pipeline() %>%
      ft_dplyr_transformer(
        tbl = df) %>%
      ft_binarizer(
        input_col = "dep_delay",
        output_col = "delayed",
        threshold = input$threshold) %>%
      ft_bucketizer(
        input_col = "sched_dep_time",
        output_col = "hours",
        splits = c(400, 800, 1200, 1600, 2000, 2400)) %>%
      ft_r_formula(
        delayed ~ month + day + hours + distance) %>%
      ml_logistic_regression()
  })


  # Partition the data into training and testing
  partitioned_flights <- reactive({
    sdf_partition(
      flights_tbl,
      training = input$training / 100,
      testing = input$testing / 100,
      rest = (100 - input$testing - input$training) / 100)
  })

  # Get Predictions from model fit
  predictions <- reactive({
    reactive_flights_pipeline() %>%
    ml_fit(
      partitioned_flights()$training) %>%
    ml_transform(
      partitioned_flights()$testing)
  })

  output$table <- renderTable({
    predictions() %>%
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
  })

  # Area under ROC
  output$aoc <- renderText({
    ml_binary_classification_evaluator(predictions())
  })
  # RMSE
  output$rmse <- renderText({
    ml_regression_evaluator(predictions())
  })

}

shinyApp(ui, server)
