# Equity Pair Trading Pipeline

If you want to set up your own pipeline in BigQuery.

1. Create a new google cloud project
2. Create a new bigquery dataset
3. Amend the script `create-db.sql` to point to your datasets and project
4. Run the script `create-db.sql` to create the tables and views required 
5. Run the other scripts to generate the stored procedures to do the transformations
6. To process a year of data (say 2021) run `CALL rw-algotrader.equity_statarb.load_features(2021);`
