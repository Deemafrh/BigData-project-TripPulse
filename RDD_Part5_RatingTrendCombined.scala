// 3.5 Rating + Trend Combined Analysis


// Input (Phase 2, Transformed Data):
//   business_features      -> one row per business: name, metro, rating_in_data, reviews_in_data
//   checkin_trend_features -> one row per business per month: trend_vs_metro
// Output:
//   Phase3/part5_rising_stars (text file, one business per line)

java.util.Locale.setDefault(java.util.Locale.US)

import org.apache.hadoop.fs.{FileSystem, Path}

val BASE = sys.env.getOrElse("TP_BASE", "C:/YelpProcessed")
val OUT  = s"$BASE/Phase3/part5_rising_stars"

// highly rated = average review rating >= 4.0 from at least 50 reviews, still open
val MIN_RATING  = 4.0
val MIN_REVIEWS = 50L

// trend is measured over 2021 (last full year in the data);
// a business needs at least 3 months with a trend value so one odd month cannot decide it
val TREND_YEAR       = 2021
val MIN_TREND_MONTHS = 3

// ---------------------------------------------------------------------------
// Business ratings
// ---------------------------------------------------------------------------
val bizRows = (spark.read.parquet(s"$BASE/Transformed Data/business_features")
  .select("business_id", "name", "metro", "rating_in_data", "reviews_in_data",
          "is_open", "geo_suspect_flag")
  .rdd)

// T1 map: (business_id, (name, metro, rating, reviews, is_open, geo_suspect))
val ratingRDD = bizRows.map { r =>
  (r.getString(0),
   (r.getString(1),
    r.getString(2),
    if (r.isNullAt(3)) 0.0   else r.getDouble(3),
    if (r.isNullAt(4)) 0L    else r.getLong(4),
    if (r.isNullAt(5)) 0L    else r.getLong(5),
    if (r.isNullAt(6)) false else r.getBoolean(6)))
}.cache()

// T2 filter: keep highly rated, open businesses with a trusted location
val goodBiz = ratingRDD.filter { case (_, (_, _, rating, reviews, isOpen, geoSuspect)) =>
  rating >= MIN_RATING && reviews >= MIN_REVIEWS && isOpen == 1L && !geoSuspect
}

// ---------------------------------------------------------------------------
// Business trend in 2021
// trend_vs_metro = business 3-month trend - metro 3-month trend (from Phase 2)
//   > 0 : the business grew faster than its metro that month
//   < 0 : it grew slower (or fell faster) than its metro
// ---------------------------------------------------------------------------
val trendRows = (spark.read.parquet(s"$BASE/Transformed Data/checkin_trend_features")
  .select("business_id", "chk_year", "trend_vs_metro", "checkin_count")
  .rdd)

// T2 filter: 2021 months that have a trend value
// (geo-suspect businesses are already removed on the rating side, in goodBiz)
val trend2021 = trendRows.filter { r =>
  !r.isNullAt(1) && r.getInt(1) == TREND_YEAR && !r.isNullAt(2)
}

// T1 map: (business_id, (trend_vs_metro, 1 month, check-ins))
val trendPairs = trend2021.map(r => (r.getString(0), (r.getDouble(2), 1, if (r.isNullAt(3)) 0L else r.getLong(3))))

// T3 reduceByKey: (business_id, (sum of trend, number of months, total check-ins))
val trendTotals = trendPairs.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

// T2 filter + T4 mapValues: average trend for businesses with enough months
val trendRDD = (trendTotals
  .filter { case (_, (_, months, _)) => months >= MIN_TREND_MONTHS }
  .mapValues { case (sumTrend, months, checkins) => (sumTrend / months, months, checkins) }
  .cache())

// ---------------------------------------------------------------------------
// Combine rating + trend
// ---------------------------------------------------------------------------

// T5 join: (business_id, ((name, metro, rating, reviews, isOpen, geo), (avgTrend, months, checkins)))
val goodWithTrend = goodBiz.join(trendRDD).cache()

// T2 filter + T1 map: rising stars = highly rated AND growing faster than their metro
val risingStars = (goodWithTrend
  .filter { case (_, (_, (avgTrend, _, _))) => avgTrend > 0 }
  .map { case (id, ((name, metro, rating, reviews, _, _), (avgTrend, months, checkins))) =>
    (id, name, metro, rating, reviews, avgTrend, months, checkins) }
  .cache())

// A1 count: sizes of each group, used for the comparison
val numBusinesses     = ratingRDD.count()
val numGood           = goodBiz.count()
val numWithTrend      = trendRDD.count()
val numPositive       = trendRDD.filter { case (_, (t, _, _)) => t > 0 }.count()
val numGoodWithTrend  = goodWithTrend.count()
val numRising         = risingStars.count()

// A2 reduce: average trend of all businesses vs highly rated ones, and rating of rising stars
val avgTrendAll    = trendRDD.map(_._2._1).reduce(_ + _) / numWithTrend
val avgTrendGood   = goodWithTrend.map(_._2._2._1).reduce(_ + _) / numGoodWithTrend
val avgRatingStars = risingStars.map(_._4).reduce(_ + _) / numRising

// T6 distinct + A1 count: how many metros have at least one rising star
val numMetros = risingStars.map(_._3).distinct().count()

// T1 map + T3 reduceByKey + T7 sortBy + A3 collect: rising stars per metro
val perMetro = (risingStars
  .map(x => (x._3, 1L))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)
  .collect())

// T7 sortBy + A4 take: top 10 rising stars by average trend
val top10 = risingStars.sortBy(_._6, ascending = false).take(10)

// T7 sortBy + T1 map + T8 coalesce + A5 saveAsTextFile: full list, one business per line
// (names can contain commas, so " | " is used as the separator)
FileSystem.get(sc.hadoopConfiguration).delete(new Path(OUT), true)
(risingStars
  .sortBy(_._6, ascending = false)
  .map { case (id, name, metro, rating, reviews, avgTrend, months, checkins) =>
    f"$id | $name | $metro | $rating%.2f | $reviews | $avgTrend%.3f | $months | $checkins" }
  .coalesce(1)
  .saveAsTextFile(OUT))

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------
def pct(a: Long, b: Long): Double = 100.0 * a / b

println("=" * 78)
println(f"Businesses:                                        $numBusinesses%,d")
println(f"Highly rated (>= $MIN_RATING%.1f, >= $MIN_REVIEWS%d reviews, open):     $numGood%,d")
println(f"Businesses with a $TREND_YEAR%d trend (>= $MIN_TREND_MONTHS%d months):         $numWithTrend%,d")
println(f"  growing faster than their metro:                 $numPositive%,d (${pct(numPositive, numWithTrend)}%.1f%%)")
println(f"Highly rated businesses with a $TREND_YEAR%d trend:          $numGoodWithTrend%,d")
println(f"  growing faster than their metro (rising stars):  $numRising%,d (${pct(numRising, numGoodWithTrend)}%.1f%%)")
println(f"Average trend_vs_metro, all businesses:            $avgTrendAll%.4f")
println(f"Average trend_vs_metro, highly rated:              $avgTrendGood%.4f")
println(f"Average rating of rising stars:                    $avgRatingStars%.2f")
println(f"Metros with at least one rising star:              $numMetros%d")
println("=" * 78)

println("\nRising stars per metro")
println(f"${"Metro"}%-15s ${"Rising stars"}%13s")
perMetro.foreach { case (metro, n) => println(f"$metro%-15s $n%,13d") }

println(s"\nTop 10 rising stars (average trend_vs_metro in $TREND_YEAR)")
println(f"${"Rank"}%-5s ${"Name"}%-32s ${"Metro"}%-14s ${"Rating"}%6s ${"Reviews"}%8s ${"Trend"}%7s")
top10.zipWithIndex.foreach { case ((_, name, metro, rating, reviews, avgTrend, _, _), i) =>
  println(f"${i + 1}%-5d ${name.take(32)}%-32s $metro%-14s $rating%6.2f $reviews%,8d $avgTrend%7.3f")
}

println("\nSaved: " + OUT)
