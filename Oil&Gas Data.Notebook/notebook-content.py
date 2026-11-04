# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c8ab76d-63a1-4806-a8cc-535bba0b5555",
# META       "default_lakehouse_name": "LH_API_INGESTION",
# META       "default_lakehouse_workspace_id": "ea224e1d-5a3b-4811-b9c3-7d78c27f52c9",
# META       "known_lakehouses": [
# META         {
# META           "id": "2c8ab76d-63a1-4806-a8cc-535bba0b5555"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

SCALE = 0.01          # 0.01 = ~19M fact rows (smoke test). 1.0 = full ~1.9B rows
N_SECURITY  = int(500_000 * max(SCALE, 0.02))
N_ISSUER    = int(150_000 * max(SCALE, 0.02))
N_COMMODITY = 25_000
N_INDEX     = 10_000
START, END  = "2000-01-03", "2026-10-30"

spark.conf.set("spark.sql.parquet.vorder.default", "true")          # V-Order on write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")  # fewer, bigger files
spark.conf.set("spark.sql.shuffle.partitions", "800")
SCHEMA = "gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SECTORS = ["Energy","Materials","Industrials","Consumer Disc.","Consumer Staples","Health Care",
           "Financials","Info Tech","Comm. Services","Utilities","Real Estate"]
REGIONS = [("NA","United States"),("NA","Canada"),("EMEA","United Kingdom"),("EMEA","Germany"),
           ("EMEA","France"),("EMEA","UAE"),("APAC","India"),("APAC","Japan"),("APAC","China"),
           ("APAC","Australia"),("LATAM","Brazil"),("LATAM","Mexico")]
RATINGS = ["AAA","AA+","AA","AA-","A+","A","A-","BBB+","BBB","BBB-","BB+","BB","BB-",
           "B+","B","B-","CCC+","CCC","CCC-","CC","C","D"]

def lit_arr(xs): return F.array(*[F.lit(x) for x in xs])
def pick(xs, seed):  # random element
    return F.element_at(lit_arr(xs), (F.floor(F.rand(seed)*len(xs))+1).cast("int"))

# dim_date
dim_date = (spark.sql(f"SELECT explode(sequence(to_date('{START}'), to_date('{END}'), interval 1 day)) d")
  .withColumn("date_key", F.date_format("d","yyyyMMdd").cast("int"))
  .withColumn("year", F.year("d")).withColumn("quarter", F.quarter("d"))
  .withColumn("month", F.month("d")).withColumn("month_name", F.date_format("d","MMM"))
  .withColumn("dow", F.dayofweek("d")).withColumn("is_trading_day", (F.dayofweek("d").between(2,6)).cast("boolean"))
  .withColumnRenamed("d","date"))
dim_date.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_date")

# dim_rating_scale
rs = spark.createDataFrame([(i+1, r, "IG" if i<10 else ("Default" if r=="D" else "HY")) for i,r in enumerate(RATINGS)],
                           "rating_key int, rating string, grade_class string")
rs.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_rating_scale")

# dim_geo
geo = spark.createDataFrame([(i+1,)+g for i,g in enumerate(REGIONS)], "geo_key int, region string, country string")
geo.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_geo")

# dim_issuer
dim_issuer = (spark.range(1, N_ISSUER+1).withColumnRenamed("id","issuer_key")
  .withColumn("issuer_name", F.concat(F.lit("Issuer "), F.lpad(F.col("issuer_key").cast("string"),6,"0")))
  .withColumn("sector", pick(SECTORS, 11))
  .withColumn("geo_key", (F.floor(F.rand(12)*len(REGIONS))+1).cast("int"))
  .withColumn("issuer_type", pick(["Corporate","Financial","Sovereign","Municipal","Structured"], 13))
  .withColumn("is_listed", F.rand(14) < 0.45))
dim_issuer.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_issuer")

# dim_security
dim_security = (spark.range(1, N_SECURITY+1).withColumnRenamed("id","security_key")
  .withColumn("ticker", F.concat(F.lit("T"), F.lpad(F.col("security_key").cast("string"),6,"0")))
  .withColumn("issuer_key", (F.floor(F.rand(21)*N_ISSUER)+1).cast("int"))
  .withColumn("asset_class", pick(["Equity","Equity","Equity","Bond","ETF"], 22))
  .withColumn("currency", pick(["USD","EUR","GBP","JPY","INR","CNY"], 23))
  .withColumn("base_price", F.round(F.exp(F.rand(24)*5+1.5),2))
  .withColumn("shares_out_m", F.round(F.exp(F.rand(25)*6+1),1)))
dim_security.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_security")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

trading = spark.table(f"{SCHEMA}.dim_date").filter("is_trading_day").select("date_key","year","date")
sec = spark.table(f"{SCHEMA}.dim_security").select("security_key","base_price","shares_out_m")

# Cross join is the fastest way to a dense price panel. Write one year at a time to keep shuffles small.
for yr in range(2007, 2029):
    d = trading.filter(F.col("year")==yr)
    px = (sec.crossJoin(d)
      .withColumn("r", F.rand(100+yr)-0.5)
      .withColumn("close", F.round(F.col("base_price")*F.exp(F.col("r")*0.08 + (F.col("year")-2000)*0.01),2))
      .withColumn("open",  F.round(F.col("close")*(1+(F.rand(200+yr)-0.5)*0.01),2))
      .withColumn("high",  F.round(F.greatest("open","close")*(1+F.rand(300+yr)*0.01),2))
      .withColumn("low",   F.round(F.least("open","close")*(1-F.rand(400+yr)*0.01),2))
      .withColumn("volume",(F.exp(F.rand(500+yr)*7+8)).cast("long"))
      .withColumn("market_cap_m", F.round(F.col("close")*F.col("shares_out_m"),1))
      .withColumnRenamed("year","trade_year")
      .select("security_key","date_key","trade_year","open","high","low","close","volume","market_cap_m"))
    (px.write.mode("overwrite" if yr==2000 else "append").format("delta")
       .partitionBy("trade_year").saveAsTable(f"{SCHEMA}.fact_price_daily"))
    print("done", yr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

N_EVENTS = int(60_000_000 * SCALE)
ev = (spark.range(N_EVENTS).withColumnRenamed("id","event_id")
  .withColumn("issuer_key", (F.floor(F.rand(1)*N_ISSUER)+1).cast("int"))
  .withColumn("date_key", F.date_format(F.date_add(F.lit("2000-01-03"), (F.rand(2)*9800).cast("int")),"yyyyMMdd").cast("int"))
  .withColumn("action_year", (F.col("date_key")/10000).cast("int"))
  .withColumn("old_rating_key", (F.floor(F.rand(3)*21)+1).cast("int"))
  .withColumn("notch", (F.floor(F.rand(4)*7)-3).cast("int"))   # -3 .. +3
  .withColumn("new_rating_key", F.least(F.lit(22), F.greatest(F.lit(1), F.col("old_rating_key")+F.col("notch"))))
  .withColumn("action_type", F.when(F.col("notch")>0,"Downgrade").when(F.col("notch")<0,"Upgrade").otherwise("Affirmed"))
  .withColumn("outlook", pick(["Stable","Positive","Negative","Developing"], 5)))
ev.write.mode("overwrite").format("delta").partitionBy("action_year").saveAsTable(f"{SCHEMA}.fact_rating_event")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

METRICS = ["Revenue","EBITDA","NetIncome","TotalDebt","Cash","CapEx","FreeCashFlow"]
q = spark.sql("SELECT DISTINCT year*10+quarter AS period_key, year AS fiscal_year FROM gold.dim_date")
iss = spark.table(f"{SCHEMA}.dim_issuer").select("issuer_key")
m = spark.createDataFrame([(i+1,x) for i,x in enumerate(METRICS)],"metric_key int, metric string")
fund = (iss.crossJoin(q).crossJoin(m)
  .withColumn("value_usd_m", F.round(F.exp(F.rand(7)*8+2)*F.when(F.col("metric")=="NetIncome",0.1).otherwise(1),2)))
(fund.select("issuer_key","period_key","fiscal_year","metric_key","value_usd_m")
  .write.mode("overwrite").format("delta").partitionBy("fiscal_year").saveAsTable(f"{SCHEMA}.fact_fundamentals"))
spark.createDataFrame([(i+1,x) for i,x in enumerate(METRICS)],"metric_key int, metric string") \
     .write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_metric")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cm = (spark.range(1, N_COMMODITY+1).withColumnRenamed("id","commodity_key")
  .withColumn("symbol", F.concat(F.lit("PLT"), F.lpad(F.col("commodity_key").cast("string"),5,"0")))
  .withColumn("commodity", pick(["Crude Oil","Natural Gas","Power","Coal","LNG","Copper","Aluminium","Gold","Lithium","Steel"], 31))
  .withColumn("unit", pick(["USD/bbl","USD/MMBtu","USD/MWh","USD/t"], 32))
  .withColumn("base", F.round(F.exp(F.rand(33)*4+2),2)))
cm.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_commodity")

dts = spark.table(f"{SCHEMA}.dim_date").filter("is_trading_day").select("date_key","year")
fca = (cm.select("commodity_key","base").crossJoin(dts)
  .withColumn("price", F.round(F.col("base")*F.exp((F.rand(41)-0.5)*0.15),3))
  .withColumnRenamed("year","assess_year").drop("base"))
fca.write.mode("overwrite").format("delta").partitionBy("assess_year").saveAsTable(f"{SCHEMA}.fact_commodity_assessment")

ix = spark.range(1, N_INDEX+1).withColumnRenamed("id","index_key") \
  .withColumn("index_name", F.concat(F.lit("GFIP Index "), F.col("index_key").cast("string"))) \
  .withColumn("index_family", pick(["Equity","Fixed Income","Commodity","ESG","Sector"], 51))
ix.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.dim_index")

lvl = (ix.select("index_key").crossJoin(dts.select("date_key"))
  .withColumn("level", F.round(F.exp(F.rand(61)*1.5+6),2)).withColumn("daily_return", F.round((F.rand(62)-0.5)*0.04,5)))
lvl.write.mode("overwrite").format("delta").saveAsTable(f"{SCHEMA}.fact_index_level")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["fact_price_daily","fact_fundamentals","fact_rating_event",
          "fact_commodity_assessment","fact_index_level"]:
    spark.sql(f"OPTIMIZE gold.{t} VORDER")      # compact + V-Order
spark.sql("VACUUM gold.fact_price_daily RETAIN 168 HOURS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
