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

# Packages needs ----------------------------------------------------------

library(arrow)
library(dplyr)
library(conbenchcoms) ## remotes::install_github("conbench/conbenchcoms", dependencies = TRUE)
library(ggplot2)
library(lubridate)
library(tidyr)
library(forcats)
library(geomtextpath)
library(gh)

# Monitoring Performance Changes in Arrow ---------------------------------

## change plot
Sys.unsetenv("CONBENCH_URL")

## history of query 22
commits_gh_raw <- gh::gh(
  "/repos/apache/arrow/commits",
  since = "2022-04-15T00:00:00",
  until = "2022-08-19T00:00:00",
  .limit = Inf
)

## release dates
release_dates <- data.frame(
  release_date = c(as.Date("2022-05-02"), as.Date("2022-08-10")),
  version = c("Arrow 8.0.0", "Arrow 9.0.0"),
  hjust_num = c(0.1, 0.9)
)

cpp_commits_gh <- data.frame(
  commits_gh_sha = vapply(commits_gh_raw, "[[", "", "sha"),
  commits_gh_datetime = vapply(seq_along(commits_gh_raw), \(x) commits_gh_raw[[x]]$commit$author$date, character(1)),
  commit_message = vapply(seq_along(commits_gh_raw), \(x) commits_gh_raw[[x]]$commit$message, character(1))
) %>%
  mutate(commits_gh_date = ymd_hms(commits_gh_datetime)) %>%
  filter(grepl("\\[C\\+\\+\\]", commit_message)) %>%
  filter(!grepl("^MINOR", commit_message)) %>%
  group_by(commits_gh_date = as.Date(commits_gh_datetime)) %>%
  filter(commits_gh_datetime == max(commits_gh_datetime)) %>% ## last commit of the day
  as_tibble()

## on just one piece of hardware
history_join_runs <- runs(cpp_commits_gh$commits_gh_sha) %>%
  filter(hardware.name == "ursa-i9-9960x") %>%
  mutate(commit.timestamp = ymd_hms(commit.timestamp))

## this step will take some time
history_join_benchmarks <- benchmarks(history_join_runs$id)


history_join_benchmarks %>%
  filter(
    tags.query_id == "TPCH-22",
    tags.format %in% "parquet",
    tags.scale_factor == 10
  ) %>%
  left_join(history_join_runs, by = c("run_id" = "id")) %>%
  mutate(commit.timestamp = as.Date(commit.timestamp)) %>%
  ggplot(aes(x = commit.timestamp, y = stats.mean)) +
  geom_line(colour = "#005050", size = 1.1) +
  geom_point(colour = "#005050", alpha = 0.8, size = 1.1) +
  geom_textvline(data = release_dates,
                aes(
                  xintercept = release_date,
                  label = version,
                  hjust = hjust_num),
                size = 6, linetype = 2, family = "Work Sans"
                ) +
  scale_x_date() +
  labs(
    x = "Benchmark Date",
    y = "Time to Complete the Query (s)",
    title = "Query 22 Timings - Arrow release dates marked by vertical lines",
  ) +
  theme_minimal(base_family = "Work Sans") +
  theme(
    plot.title.position = "plot",
    plot.background = element_rect(fill = "white"),
    axis.text = element_text(size = 14),
    axis.title = element_text(size = 14)
  )

ggsave("history.png", width = 2560, height = 1707, units = "px")
ggsave("history.jpeg", width = 2560, height = 1707, units = "px")


## Let's look at all the queries

## before and after where benchmarks ran
shas <- c(
  "old" = "0024962ff761d1d5f3a63013e67886334f1e57ca",
  "new" = "ee2e9448c8565820ba38a2df9e44ab6055e5df1d"
  )

join_runs <- runs(shas) %>%
  filter(hardware.name == "ursa-i9-9960x", !has_errors) %>%
  mutate(commit.timestamp = ymd_hms(commit.timestamp))


join_benchmarks <- benchmarks(join_runs$id) %>%
  filter(
    tags.format == "parquet",
    tags.scale_factor == 10
  ) %>%
  left_join(join_runs, by = c("run_id" = "id")) %>%
  mutate(state = case_when(
    commit.sha == "0024962ff761d1d5f3a63013e67886334f1e57ca" ~ "old",
    commit.sha == "ee2e9448c8565820ba38a2df9e44ab6055e5df1d" ~ "new",
    TRUE ~ NA_character_
  ))

query_with_joins <- c(
  "TPCH-02", "TPCH-03", "TPCH-04", "TPCH-05",
  "TPCH-07", "TPCH-08", "TPCH-09", "TPCH-10",
  "TPCH-11", "TPCH-12", "TPCH-13", "TPCH-14",
  "TPCH-15", "TPCH-16", "TPCH-17", "TPCH-18",
  "TPCH-19", "TPCH-20", "TPCH-21", "TPCH-22"
)


## percent change
join_benchmarks %>%
  select(tags.query_id, state, stats.mean) %>%
  pivot_wider(names_from = state, values_from = stats.mean) %>%
  mutate(per_change = old / new) %>%
  mutate(query_with_joins = ifelse(tags.query_id %in% query_with_joins, "Query with joins", "Query without a join")) %>%
  mutate(tags.query_id = gsub("TPCH-", "", tags.query_id)) %>%
  ggplot(aes(y = fct_reorder(tags.query_id, per_change), x = per_change)) +
  labs(
    x = "Percent Change", y = "Query",
    title = "Percent Change in Query Performance after Hash Join Improvement"
  ) +
  scale_x_continuous(labels = ~ scales::percent(.x, big.mark = ",")) +
  scale_fill_manual(values = c("#005050", "#3BD9FF")) +
  geom_col(aes(fill = query_with_joins), alpha = 0.8) +
  geom_text(
    aes(
      x = per_change - 0.18,
      label = scales::percent(per_change, accuracy = 1, big.mark = ",")
      ),
    size = 4,
    colour = "white",
    family = "Work Sans") +
  theme_minimal(base_family = "Work Sans") +
  theme(
    plot.title.position = "plot",
    plot.background = element_rect(fill = "white"),
    legend.title = element_blank(),
    legend.position = "bottom",
    axis.text = element_text(size = 12),
    axis.title = element_text(size = 12)
  )

ggsave("all_queries.png", width = 2560, height = 1707, units = "px")
ggsave("all_queries.jpeg", width = 2560, height = 1707, units = "px")