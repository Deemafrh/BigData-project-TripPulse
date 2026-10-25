java.util.Locale.setDefault(java.util.Locale.US)

val BASE = sys.env.getOrElse("TP_BASE", "D:/IT462/processed")

val businessDF = spark.read.parquet(s"$BASE/Transformed Data/business_features")
businessDF.createOrReplaceTempView("business_features")

// a business needs at least this many reviews to be ranked; the same number is the weight m in the weighted rating
val MIN_REVIEWS = 100
// places a visitor would go to (Yelp parent categories)
val VISITOR_CATS = "Restaurants|Food|Nightlife|Arts & Entertainment|Active Life|Hotels & Travel"

// open visitor places with enough reviews, scored with a weighted rating that pulls small samples toward the metro average
spark.sql(s"""
  SELECT business_id, name, city, metro, reviews_in_data AS reviews, avg_review_stars_in_data AS avg_rating,
         (reviews_in_data * avg_review_stars_in_data
           + $MIN_REVIEWS * AVG(avg_review_stars_in_data) OVER (PARTITION BY metro))
           / (reviews_in_data + $MIN_REVIEWS) AS weighted_rating
  FROM business_features
  WHERE is_open = 1
    AND reviews_in_data >= $MIN_REVIEWS
    AND categories RLIKE '$VISITOR_CATS'
    AND metro IS NOT NULL
""").createOrReplaceTempView("scored_places")

// Q2a: top 5 places in each metro, with their position in the overall ranking
val topPerMetro = spark.sql("""
  SELECT metro, rank_in_metro, overall_rank, name, city, reviews,
         ROUND(avg_rating, 2) AS avg_rating, ROUND(weighted_rating, 3) AS weighted_rating
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY metro ORDER BY weighted_rating DESC, reviews DESC) AS rank_in_metro,
           RANK()       OVER (ORDER BY weighted_rating DESC) AS overall_rank
    FROM scored_places
  ) ranked
  WHERE rank_in_metro <= 5
  ORDER BY metro, rank_in_metro
""")
println("\nQ2a: Top 5 places per metro (open, at least " + MIN_REVIEWS + " reviews)")
topPerMetro.show(60, false)

// Q2b: one global top 10 for comparison
val topOverall = spark.sql("""
  SELECT name, city, metro, reviews, ROUND(avg_rating, 2) AS avg_rating, ROUND(weighted_rating, 3) AS weighted_rating
  FROM scored_places
  ORDER BY weighted_rating DESC, reviews DESC
  LIMIT 10
""")
println("Q2b: Top 10 places overall")
topOverall.show(false)

// Q2c: the place that would be first in each metro if ranked by average rating alone, and where the weighted rating puts it
val firstByAverage = spark.sql("""
  SELECT metro, name, reviews, ROUND(avg_rating, 2) AS avg_rating, rank_by_weighted
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY metro ORDER BY avg_rating DESC, reviews DESC) AS rank_by_avg,
           ROW_NUMBER() OVER (PARTITION BY metro ORDER BY weighted_rating DESC, reviews DESC) AS rank_by_weighted
    FROM scored_places
  ) ranked
  WHERE rank_by_avg = 1
  ORDER BY metro
""")
println("Q2c: First place by average rating alone, and its rank by weighted rating")
firstByAverage.show(false)

// places that passed the filters in each metro
println("Ranked places in total: " + spark.table("scored_places").count())
println("Ranked places per metro")
spark.sql("""
  SELECT metro, COUNT(*) AS places, ROUND(AVG(avg_rating), 2) AS metro_avg_rating
  FROM scored_places
  GROUP BY metro
  ORDER BY places DESC
""").show(false)
