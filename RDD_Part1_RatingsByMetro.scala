java.util.Locale.setDefault(java.util.Locale.US)

val BASE = sys.env.getOrElse("TP_BASE", "D:/IT462/processed")

// one row per review with its rating and the metro of the business (from Phase 2)
val reviewRows = spark.read.parquet(s"$BASE/Transformed Data/model_table")
  .select("biz_metro", "label_stars")
  .na.drop()
  .rdd

// T1 map: (metro, rating)
val metroRating = reviewRows.map(r => (r.getString(0), r.getDouble(1)))

// T2 map: (metro, (rating, 1)) so ratings and counts can be added together
val metroPairs = metroRating.map { case (metro, stars) => (metro, (stars, 1L)) }

// T3 reduceByKey: (metro, (sum of ratings, number of reviews))
val metroTotals = metroPairs.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

// T4 map: (metro, number of reviews, average rating)
val metroStats = metroTotals
  .map { case (metro, (sum, cnt)) => (metro, cnt, sum / cnt) }
  .cache()

// A1 count
val totalReviews = metroRating.count()
val numMetros = metroStats.count()
println(s"\nTotal reviews: $totalReviews")
println(s"Number of metros: $numMetros")

// T5 sortBy + A2 take: most active metros
val byActivity = metroStats.sortBy(_._2, ascending = false).take(numMetros.toInt)
println("\nMetros ranked by number of reviews")
println(f"${"Rank"}%-5s ${"Metro"}%-15s ${"Reviews"}%12s ${"Share"}%8s ${"Avg rating"}%11s")
byActivity.zipWithIndex.foreach { case ((metro, cnt, avg), i) =>
  println(f"${i + 1}%-5d $metro%-15s $cnt%,12d ${100.0 * cnt / totalReviews}%7.1f%% $avg%11.2f")
}

// T5 sortBy + A2 take: highest rated metros
val byRating = metroStats.sortBy(_._3, ascending = false).take(5)
println("\nTop 5 metros by average rating")
println(f"${"Rank"}%-5s ${"Metro"}%-15s ${"Avg rating"}%11s ${"Reviews"}%12s")
byRating.zipWithIndex.foreach { case ((metro, cnt, avg), i) =>
  println(f"${i + 1}%-5d $metro%-15s $avg%11.2f $cnt%,12d")
}

// the five most active metros are the ones Phase 1 selected for detailed trend analysis
println("\nFive most active metros: " + byActivity.take(5).map(_._1).mkString(", "))
