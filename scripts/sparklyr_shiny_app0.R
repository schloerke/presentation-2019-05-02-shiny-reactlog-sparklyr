

library(sparklyr)
library(shiny)
library(dplyr)
library(ggplot2)


ui <- fluidPage(

  titlePanel("Sparklyr demo"),

  sidebarLayout(
    sidebarPanel(
      # count 10-400
      # dist 0 - 2000
      # threshold 1 - 40
      # training 1 - 10
      # testing 1 - 10
    ),
    mainPanel(
      # plot output
      # model table output
      # aoc verbatim text output
      # rmse verbatim text output
    )
  )
)

sc <- spark_connect(master = "local")

# copy data in
if (!exists("flights_tbl")) {
  flights_tbl <- copy_to(sc, nycflights13::flights, "flights", overwrite = TRUE)
}


server <- function(input, output) {


}

shinyApp(ui, server)
