from bigquery_frame import BigQueryBuilder

bq = BigQueryBuilder()
from bigquery_frame.data_diff import DataframeComparator

#####################################################################################################################
# The input tables are snapshots of the table `bigquery-public-data.utility_us.country_code_iso` made at 6 days
# interval, the 2022-09-21 and 2022-09-27. I noticed that the table had been updated with obviously incorrect data:
# as you can see in the diff below, it looks like the continent_code and continent_name columns had been inverted.
# A few hours later, the table was reverted to the same version as it was on the 2022-09-21.
#####################################################################################################################

old_df = bq.table("test_us.country_code_iso_snapshot_20220921")
new_df = bq.table("test_us.country_code_iso_snapshot_20220927")
result = DataframeComparator().compare_df(old_df, new_df, join_cols=["country_name"])

result.display(show_examples=True)
# Schema: ok (10)
# diff NOT ok
# Summary:
# Row count ok: 278 rows
# 28 (10.07%) rows are identical
# 250 (89.93%) rows have changed
#   0%|          | 0/1 [00:00<?, ?it/s]
# 100%|██████████| 1/1 [00:04<00:00,  4.91s/it]
# Found the following differences:
# +----------------+---------------+---------------+--------------------+----------------+
# |    column_name | total_nb_diff |          left |              right | nb_differences |
# +----------------+---------------+---------------+--------------------+----------------+
# | continent_code |           250 |            NA |          Caribbean |             26 |
# | continent_code |           250 |            AF |     Eastern Africa |             20 |
# | continent_code |           250 |            AS |       Western Asia |             18 |
# | continent_code |           250 |            AF |     Western Africa |             17 |
# | continent_code |           250 |            EU |    Southern Europe |             16 |
# | continent_code |           250 |            EU |    Northern Europe |             15 |
# | continent_code |           250 |            SA |      South America |             14 |
# | continent_code |           250 |            AS | South-Eastern Asia |             11 |
# | continent_code |           250 |            EU |     Eastern Europe |             10 |
# | continent_code |           250 |            OC |          Polynesia |             10 |
# | continent_name |           250 |        Africa |                 AF |             58 |
# | continent_name |           250 |          Asia |                 AS |             54 |
# | continent_name |           250 |        Europe |                 EU |             53 |
# | continent_name |           250 | North America |                 NA |             39 |
# | continent_name |           250 |       Oceania |                 OC |             26 |
# | continent_name |           250 | South America |                 SA |             15 |
# | continent_name |           250 |    Antarctica |                 AN |              5 |
# +----------------+---------------+---------------+--------------------+----------------+
# Detailed examples :
# 'continent_code' : 250 rows
# +-----------------------------------+----------------------+-----------------------+----------------------+-----------------------+
# |                      country_name | left__continent_code | right__continent_code | left__continent_name | right__continent_name |
# +-----------------------------------+----------------------+-----------------------+----------------------+-----------------------+
# | Congo; Democratic Republic of the |                   AF |         Middle Africa |               Africa |                    AF |
# |                            Angola |                   AF |         Middle Africa |               Africa |                    AF |
# |                          Cameroon |                   AF |         Middle Africa |               Africa |                    AF |
# |            Congo; Republic of the |                   AF |         Middle Africa |               Africa |                    AF |
# |          Central African Republic |                   AF |         Middle Africa |               Africa |                    AF |
# |                             Gabon |                   AF |         Middle Africa |               Africa |                    AF |
# |             Sao Tome and Principe |                   AF |         Middle Africa |               Africa |                    AF |
# |                              Chad |                   AF |         Middle Africa |               Africa |                    AF |
# |                 Equatorial Guinea |                   AF |         Middle Africa |               Africa |                    AF |
# |                           Mayotte |                   AF |        Eastern Africa |               Africa |                    AF |
# +-----------------------------------+----------------------+-----------------------+----------------------+-----------------------+
# only showing top 10 rows
# 'continent_name' : 250 rows
# +-----------------------------------+----------------------+-----------------------+----------------------+-----------------------+
# |                      country_name | left__continent_code | right__continent_code | left__continent_name | right__continent_name |
# +-----------------------------------+----------------------+-----------------------+----------------------+-----------------------+
# | Congo; Democratic Republic of the |                   AF |         Middle Africa |               Africa |                    AF |
# |                            Angola |                   AF |         Middle Africa |               Africa |                    AF |
# |                          Cameroon |                   AF |         Middle Africa |               Africa |                    AF |
# |            Congo; Republic of the |                   AF |         Middle Africa |               Africa |                    AF |
# |          Central African Republic |                   AF |         Middle Africa |               Africa |                    AF |
# |                             Gabon |                   AF |         Middle Africa |               Africa |                    AF |
# |             Sao Tome and Principe |                   AF |         Middle Africa |               Africa |                    AF |
# |                              Chad |                   AF |         Middle Africa |               Africa |                    AF |
# |                 Equatorial Guinea |                   AF |         Middle Africa |               Africa |                    AF |
# |                           Mayotte |                   AF |        Eastern Africa |               Africa |                    AF |
# +-----------------------------------+----------------------+-----------------------+----------------------+-----------------------+
# only showing top 10 rows
