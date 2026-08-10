-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Constructor Standings
-- MAGIC
-- MAGIC #### Sources
-- MAGIC 1. fact_session_results
-- MAGIC 1. dim_constructors
-- MAGIC
-- MAGIC #### Output Columns
-- MAGIC 1. season
-- MAGIC 1. constructor id
-- MAGIC 1. constructor name
-- MAGIC 1. nationality
-- MAGIC 1. race starts
-- MAGIC 1. total points
-- MAGIC 1. number of wins
-- MAGIC 1. number of podiums
-- MAGIC 1. standing position

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1.gold.v_constructor_standing
AS
WITH constructor_session_summary
AS
  (SELECT r.season,
        c.constructor_id,
        c.constructor_name,
        c.nationality,
        COUNT(*) AS race_starts,
        SUM(r.points) AS total_points,
        COUNT_IF(r.is_win) AS number_of_wins,
        COUNT_IF(r.is_podium) AS number_of_podiums
    FROM formula1.gold.fact_session_results r
    JOIN formula1.gold.dim_constructors c
      ON r.constructor_id = c.constructor_id 
  GROUP BY r.season,
        c.constructor_id,
        c.constructor_name,
        c.nationality)    
SELECT season,
       constructor_id,
       constructor_name,
       nationality,
       RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing,
       race_starts,
       total_points,
       number_of_wins,
       number_of_podiums
  FROM constructor_session_summary;


-- COMMAND ----------

SELECT * FROM formula1.gold.v_constructor_standing WHERE season = 2025
