# Copyright (c) 2022, Voltron Data.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## example using tpch
library(arrow)
library(arrowbench)
library(dplyr)

temp_tpch_dir <- tempdir()
Sys.setenv(ARROWBENCH_DATA_DIR = temp_tpch_dir)
## remotes::install_github("voltrondata-labs/arrowbench", dependencies = TRUE)
## will require a working python installation and will install duckdb for you
## with the ability to generate TPC-H datasets to custom library
tpch_data <- generate_tpch(scale_factor = 0.1)

acctbal_mins <- open_dataset(tpch_data$customer) %>%
  filter(
    substr(c_phone, 1, 2) %in% c("13", "31", "23", "29", "30", "18", "17") &
      c_acctbal > 0
  ) %>%
  summarise(acctbal_min = mean(c_acctbal, na.rm = TRUE), join_id = 1L)

query_22 <- open_dataset(tpch_data$customer) %>%
  mutate(cntrycode = substr(c_phone, 1, 2), join_id = 1L) %>%
  left_join(acctbal_mins, by = "join_id") %>%
  filter(
    cntrycode %in% c("13", "31", "23", "29", "30", "18", "17") &
      c_acctbal > acctbal_min
  ) %>%
  anti_join(open_dataset(tpch_data$orders), by = c("c_custkey" = "o_custkey")) %>%
  select(cntrycode, c_acctbal) %>%
  group_by(cntrycode) %>%
  summarise(
    numcust = n(),
    totacctbal = sum(c_acctbal)
  ) %>%
  ungroup() %>%
  arrange(cntrycode) 

collect(query_22)

show_query(query_22)
