// ============================================================
// Phase 3 - RDD Operations
// Analysis: Popular Business Categories
// Question:
// Which business categories are the most common,
// and how do their average ratings compare?
// ============================================================


// ------------------------------------------------------------
// 1. Load the preprocessed business dataset
// ------------------------------------------------------------

val BUSINESS_PATH =
  System.getProperty("user.home") +
  "/Downloads/standardized_business/standardized_business"

val businessDF = spark.read.parquet(BUSINESS_PATH)

println("==============================================")
println("BUSINESS DATASET")
println("==============================================")

println("Business rows = " + businessDF.count())

businessDF
  .select("business_id", "categories", "stars")
  .show(10, false)


// ------------------------------------------------------------
// 2. Convert businesses into category-level records
//
// A business may belong to multiple categories.
// flatMap creates one record for each category.
// ------------------------------------------------------------

val categoryRDD = businessDF
  .select("business_id", "categories", "stars")
  .rdd
  .flatMap { row =>

    val businessId = row.getAs[String]("business_id")
    val categories = row.getAs[String]("categories")
    val stars = row.getAs[Double]("stars")

    categories
      .split(",")
      .map(_.trim)
      .filter(category =>
        category.nonEmpty && category != "Unknown"
      )
      .distinct
      .map(category =>
        (category, businessId, stars)
      )
  }


// ------------------------------------------------------------
// 3. Validate the expanded category RDD
// Actions: count and take
// ------------------------------------------------------------

println()
println("==============================================")
println("CATEGORY RDD")
println("==============================================")

println("Category records = " + categoryRDD.count())

println()
println("Sample category records:")

categoryRDD
  .take(10)
  .foreach(println)


// ------------------------------------------------------------
// 4. Count businesses in each category
//
// map:
// (category, businessId, stars)
// -> (category, 1)
//
// reduceByKey:
// Adds the counts for each category.
// ------------------------------------------------------------

val categoryCounts = categoryRDD
  .map {
    case (category, businessId, stars) =>
      (category, 1L)
  }
  .reduceByKey(_ + _)


// ------------------------------------------------------------
// 5. Rank categories by popularity
// ------------------------------------------------------------

val categoriesByPopularity = categoryCounts
  .sortBy(
    {
      case (category, count) => count
    },
    ascending = false
  )

println()
println("==============================================")
println("TOP 20 MOST POPULAR BUSINESS CATEGORIES")
println("==============================================")

categoriesByPopularity
  .take(20)
  .foreach {
    case (category, count) =>
      println(
        f"$category%-35s Businesses = $count%,d"
      )
  }


// ------------------------------------------------------------
// 6. Calculate average rating and number of businesses
//    for each category
//
// map:
// category -> (rating, 1)
//
// reduceByKey:
// Calculates rating sum and business count.
//
// mapValues:
// Converts rating sum into average rating.
// ------------------------------------------------------------

val categoryRatingStats = categoryRDD
  .map {
    case (category, businessId, stars) =>
      (category, (stars, 1L))
  }
  .reduceByKey {
    case ((ratingSum1, count1), (ratingSum2, count2)) =>
      (
        ratingSum1 + ratingSum2,
        count1 + count2
      )
  }
  .mapValues {
    case (ratingSum, count) =>
      (
        ratingSum / count,
        count
      )
  }


// ------------------------------------------------------------
// 7. Rank categories by popularity while showing
//    their average ratings
// ------------------------------------------------------------

val popularCategoryStats = categoryRatingStats
  .sortBy(
    {
      case (category, (avgRating, count)) =>
        count
    },
    ascending = false
  )

println()
println("==============================================")
println("TOP 20 CATEGORIES: POPULARITY + AVG RATING")
println("==============================================")

popularCategoryStats
  .take(20)
  .foreach {
    case (category, (avgRating, count)) =>
      println(
        f"$category%-35s Businesses = $count%,d | Avg Rating = $avgRating%.2f"
      )
  }


// ------------------------------------------------------------
// 8. Find highly rated categories with sufficient support
//
// A threshold of at least 500 businesses is used to
// prevent very small categories from appearing at the top
// because of only a few highly rated businesses.
// ------------------------------------------------------------

val highlyRatedPopular = categoryRatingStats
  .filter {
    case (category, (avgRating, count)) =>
      count >= 500
  }
  .sortBy(
    {
      case (category, (avgRating, count)) =>
        avgRating
    },
    ascending = false
  )

println()
println("==============================================")
println("TOP 20 HIGHEST-RATED CATEGORIES")
println("(Minimum 500 businesses)")
println("==============================================")

highlyRatedPopular
  .take(20)
  .foreach {
    case (category, (avgRating, count)) =>
      println(
        f"$category%-35s Avg Rating = $avgRating%.2f | Businesses = $count%,d"
      )
  }


// ------------------------------------------------------------
// End of Popular Business Categories Analysis
// ------------------------------------------------------------