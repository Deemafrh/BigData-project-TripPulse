// 3.4 User Rating Behavior
// Student 3 — analyzes user review activity, average ratings, and high-activity users.

java.util.Locale.setDefault(java.util.Locale.US)

val BASE = sys.env.getOrElse(
  "TP_REVIEW_FEATURES",
  "/Users/renadalowais/Library/CloudStorage/GoogleDrive-renad.alowais0@gmail.com/.shortcut-targets-by-id/1QzcEo6lADa3Dj23egwp5kQwpgilXaGiy/review_features"
)

val HIGH_ACTIVITY_THRESHOLD = 20

val reviewRows = spark.read.parquet(BASE)
  .select("user_id", "stars")
  .na.drop()
  .rdd

// map: each review -> (user_id, (stars, 1L))
val userStarPairs = reviewRows.map { row =>
  val userId = row.getString(0)
  val stars  = row.getDouble(1)
  (userId, (stars, 1L))
}

// reduceByKey: aggregate total stars and review count per user
val userAggregates = userStarPairs
  .reduceByKey((a: (Double, Long), b: (Double, Long)) => (a._1 + b._1, a._2 + b._2))
  .cache()

// mapValues: compute per-user average rating
val userAverages = userAggregates.mapValues { case (totalStars, reviewCount) =>
  (totalStars / reviewCount, reviewCount)
}

val totalUsers = userAggregates.count()

// filter + count: identify high-activity users (>= HIGH_ACTIVITY_THRESHOLD reviews)
val highActivityUsers = userAverages.filter { case (_, (_, reviewCount)) =>
  reviewCount >= HIGH_ACTIVITY_THRESHOLD
}.cache()

val highActivityCount = highActivityUsers.count()

// sortBy + take: sample of high-activity users, most active first
val topExamples = highActivityUsers
  .sortBy({ case (_, (_, reviewCount)) => reviewCount }, ascending = false)
  .take(10)

println("=" * 70)
println(f"Total distinct users (reviewers): $totalUsers%,d")
println(f"High-activity users (>= $HIGH_ACTIVITY_THRESHOLD%d reviews): $highActivityCount%,d (${highActivityCount * 100.0 / totalUsers}%.2f%%)")
println("=" * 70)
println(f"${"user_id"}%-25s ${"review_count"}%13s ${"avg_rating"}%11s")
println("-" * 52)
topExamples.foreach { case (userId, (avgStars, reviewCount)) =>
  println(f"${userId}%-25s ${reviewCount}%13d ${avgStars}%11.2f")
}
println("=" * 70)

System.exit(0)
