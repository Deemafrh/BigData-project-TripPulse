java.util.Locale.setDefault(java.util.Locale.US)

val BASE = sys.env.getOrElse(
  "TP_BUSINESS_FEATURES",
  "/Users/renadalowais/Library/CloudStorage/GoogleDrive-renad.alowais0@gmail.com/.tmp/17817/business_features"
)

val MIN_BUSINESSES = 50

val businessDF = spark.read.parquet(BASE)
businessDF.createOrReplaceTempView("business_features")

val businessCategoriesDF = spark.sql("""
  SELECT business_id, metro, stars, review_count,
         trim(category) AS category
  FROM business_features
  LATERAL VIEW explode(split(categories, ',')) t AS category
  WHERE categories IS NOT NULL AND trim(category) != '' AND trim(category) != 'Unknown'
""")
businessCategoriesDF.createOrReplaceTempView("business_categories")

println("=" * 70)
println("QUERY 1: Top categories by average rating, per metro")
println("(GROUP BY metro, category; HAVING min business count)")
println("=" * 70)

val metroCategoryQuery = spark.sql(s"""
  SELECT metro, category,
         COUNT(*) AS business_count,
         ROUND(AVG(stars), 2) AS avg_rating,
         ROUND(AVG(review_count), 0) AS avg_review_count
  FROM business_categories
  GROUP BY metro, category
  HAVING COUNT(*) >= $MIN_BUSINESSES
  ORDER BY metro, avg_rating DESC
""")
metroCategoryQuery.cache()

val metroCategoryRowCount = metroCategoryQuery.count()
println(s"Metro x category combinations meeting the threshold: $metroCategoryRowCount")
println("\nSample (top rated category per metro):")
metroCategoryQuery.createOrReplaceTempView("metro_category_stats")
spark.sql("""
  SELECT metro, category, business_count, avg_rating, avg_review_count
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY metro ORDER BY avg_rating DESC) AS rnk
    FROM metro_category_stats
  )
  WHERE rnk = 1
  ORDER BY avg_rating DESC
""").show(11, false)

println("=" * 70)
println("QUERY 2: Overall category performance across all metros")
println("(GROUP BY category; HAVING min business count)")
println("=" * 70)

val overallCategoryQuery = spark.sql(s"""
  SELECT category,
         COUNT(*) AS business_count,
         ROUND(AVG(stars), 2) AS avg_rating,
         ROUND(AVG(review_count), 0) AS avg_review_count
  FROM business_categories
  GROUP BY category
  HAVING COUNT(*) >= $MIN_BUSINESSES
  ORDER BY avg_rating DESC
""")

println("\nTop 10 highest-rated categories (sufficiently represented):")
overallCategoryQuery.show(10, false)

println("\nBottom 10 lowest-rated categories (sufficiently represented):")
overallCategoryQuery.orderBy("avg_rating").show(10, false)

System.exit(0)
