// ============================================================================
// IT 462 - Big Data Systems | TripPulse
// Phase 3 - RDD Operations
// Student 4: Local Trend Discovery
//
// Analytical question:
//   Which businesses are becoming more popular over time,
//   based on check-in activity growth?
//
// Why this matters for TripPulse:
//   The platform's second goal is discovering *emerging* places.
//   A business with 7,000 total reviews is well-known.
//   A business that doubled its check-ins in two years is a better
//   "emerging" signal. This analysis finds those businesses.
//
// RDD Transformations: map, reduceByKey, filter, sortBy, join,
//                      groupByKey, distinct
// RDD Actions:         count, take, first, reduce, collect,
//                      saveAsTextFile, foreach
// ============================================================================

import java.sql.Timestamp
import java.io.PrintWriter


// ============================================================================
// 0) PATHS
//
// If running on a different machine, change the paths in this section only.
// Everything else in the script stays the same.
//
// INPUT paths  → Phase 2 outputs
// OUTPUT paths → where Phase 3 results will be saved
// ============================================================================

// ---- INPUT: Phase 2 outputs (change the base directory for different machines)
val P2_BASE     = "D:/Documents/UNI/Z LAST TERM/BD 462/Project/P2/"
val CHECKIN_IN  = P2_BASE + "Checkin Data/standardized_checkin"
val BUSINESS_IN = P2_BASE + "Business Data/standardized_business"

// ---- OUTPUT: Phase 3 results
val P3_OUT      = "D:/Documents/UNI/Z LAST TERM/BD 462/Project/P3/Student4/"
val SUMMARY_TXT = P3_OUT + "trend_summary.txt"
val TOP_BIZ_DIR = P3_OUT + "top_growing_businesses"  // saved as RDD text file

// ---- Verify inputs exist before running
require(new java.io.File(CHECKIN_IN).exists,
  "\nPATH ERROR: Cannot find check-in data at: " + CHECKIN_IN +
  "\nCheck that Phase 2 ran correctly and the folder name matches.")
require(new java.io.File(BUSINESS_IN).exists,
  "\nPATH ERROR: Cannot find business data at: " + BUSINESS_IN)

new java.io.File(P3_OUT).mkdirs()   // create output folder if it doesn't exist
println("Paths verified. Starting analysis...\n")


// ============================================================================
// 1) LOAD DATA AND CONVERT TO RDDs
//
// Phase 3 must use the RDD API, not DataFrames.
// We load parquet files as DataFrames first (the standard way to read parquet),
// then immediately convert them to RDDs for all actual analysis work.
// ============================================================================

val checkinDF  = spark.read.parquet(CHECKIN_IN)
val businessDF = spark.read.parquet(BUSINESS_IN)

// Convert check-in data: one row per event → (business_id, year_as_int)
// We extract just the year from the timestamp because our trend metric
// compares activity across yearly windows.
val checkinRDD = checkinDF.rdd.map(row => {
  val bizId = row.getAs[String]("business_id")
  val ts    = row.getAs[Timestamp]("checkin_ts")
  val year  = ts.toLocalDateTime.getYear
  (bizId, year)
}).cache()  // cache because this RDD is reused across all 5 analyses

// Convert business data: keep only the columns we need for joining
// (business_id, name, metro, stars)
val businessRDD = businessDF.rdd.map(row => (
  row.getAs[String]("business_id"),
  (
    row.getAs[String]("name"),
    row.getAs[String]("metro"),
    row.getAs[Double]("stars")
  )
)).cache()

println("Data loaded.")
println("Check-in events : " + checkinRDD.count())   // ACTION: count
println("Businesses       : " + businessRDD.count()) // ACTION: count


// ============================================================================
// ANALYSIS 1: Check-in volume per business per year
//
// Question: How much activity does each business see in each year?
// This is the foundation for every trend calculation below.
//
// Operations: map, reduceByKey, sortBy, take, count
// ============================================================================

println("\n" + "="*60)
println("ANALYSIS 1: Check-in counts per business per year")
println("="*60)

// TRANSFORMATION map
// Each check-in row is (business_id, year). We reformat it to a key-value
// pair so reduceByKey can group by (business_id, year) together.
// Input:  (bizId, year)
// Output: ((bizId, year), 1)
val keyedCheckins = checkinRDD.map(row => ((row._1, row._2), 1))

// TRANSFORMATION reduceByKey
// Sums all the 1s that share the same (business_id, year) key.
// Input:  ((bizId, year), 1), ((bizId, year), 1), ...
// Output: ((bizId, year), total_count)
val yearlyCountRDD = keyedCheckins.reduceByKey(_ + _).cache()

// ACTION count -- how many distinct (business, year) combinations exist
val totalPairs = yearlyCountRDD.count()
println(s"Distinct (business, year) combinations: $totalPairs")
// Insight: this tells us how spread out activity is across businesses and time

// ACTION reduce -- find the data's full year range
val minYear = checkinRDD.map(_._2).reduce((a, b) => math.min(a, b))  // ACTION reduce
val maxYear = checkinRDD.map(_._2).reduce((a, b) => math.max(a, b))  // ACTION reduce
println(s"Dataset spans years $minYear to $maxYear")

// TRANSFORMATION sortBy -- sort by year descending to see most recent first
val recentFirst = yearlyCountRDD.sortBy(_._1._2, ascending = false)

// ACTION take -- print a sample of recent activity
println("\nSample: recent check-in records (first 10):")
recentFirst.take(10).foreach { case ((bizId, year), cnt) =>
  println(s"  biz=$bizId  year=$year  checkins=$cnt")
}
// Insight: confirms data is present for recent years and shows typical
// check-in volumes per business per year


// ============================================================================
// ANALYSIS 2: Compute growth rate for each business
//
// Question: Which businesses grew their check-in activity the most?
//
// Method:
//   recent   = check-ins in the 2 most recent years of the dataset
//   baseline = check-ins in the 2 years before that
//   growth % = (recent - baseline) / baseline * 100
//
// We require a minimum of 10 baseline check-ins to exclude businesses that
// appear to "grow" only because they went from 1 check-in to 3.
//
// Operations: filter, map, reduceByKey, join, count
// ============================================================================

println("\n" + "="*60)
println("ANALYSIS 2: Growth rate computation")
println("="*60)

val recentStart   = maxYear - 1   // 2-year recent window: e.g. 2020-2021
val baselineStart = maxYear - 3   // 2-year baseline window: e.g. 2018-2019
val baselineEnd   = maxYear - 2

println(s"Recent window:   $recentStart – $maxYear")
println(s"Baseline window: $baselineStart – $baselineEnd")

// TRANSFORMATION filter (on checkinRDD)
// Keep only check-ins that fall within the recent window,
// then count them per business.
val recentRDD = checkinRDD
  .filter(row => row._2 >= recentStart)   // filter: year in recent window
  .map(row => (row._1, 1))               // map: (bizId, 1)
  .reduceByKey(_ + _)                    // reduceByKey: total recent checkins

// TRANSFORMATION filter (baseline window)
// Same idea for the baseline window.
val baselineRDD = checkinRDD
  .filter(row => row._2 >= baselineStart && row._2 <= baselineEnd) // filter: baseline years
  .map(row => (row._1, 1))                                          // map: (bizId, 1)
  .reduceByKey(_ + _)                                               // reduceByKey: total baseline

// ACTION count -- how many businesses appear in each window
println("Businesses with recent check-ins:   " + recentRDD.count())
println("Businesses with baseline check-ins: " + baselineRDD.count())

// TRANSFORMATION join
// Inner join matches each business's recent count with its baseline count.
// Businesses present in only one window are excluded.
// Input:  (bizId, recentCount) and (bizId, baselineCount)
// Output: (bizId, (recentCount, baselineCount))
val joinedRDD = recentRDD.join(baselineRDD)
// Insight: the join is the bridge between two time slices of the same data

// TRANSFORMATION filter + map
// Filter out businesses with too-small baseline (noise), then
// compute growth percentage for the rest.
val growthRDD = joinedRDD
  .filter { case (_, (_, baseline)) => baseline >= 10 }  // filter: min baseline threshold
  .map { case (bizId, (recent, baseline)) =>              // map: compute growth %
    val growthPct = (recent - baseline).toDouble / baseline * 100.0
    (bizId, (recent, baseline, growthPct))
  }
  .cache()  // reused in analyses 3, 4, 5

// ACTION count
println("Businesses with enough data to compute growth: " + growthRDD.count())

// Sample
println("\nSample growth rates:")
growthRDD.take(5).foreach { case (bizId, (r, b, g)) =>
  println(f"  $bizId  recent=$r  baseline=$b  growth=$g%.1f%%")
}


// ============================================================================
// ANALYSIS 3: Top 20 fastest growing businesses
//
// Question: Which specific businesses are trending the most?
//
// Operations: sortBy, take, first
// ============================================================================

println("\n" + "="*60)
println("ANALYSIS 3: Top 20 fastest growing businesses")
println("="*60)

// TRANSFORMATION sortBy
// Sorts the entire growthRDD by growth percentage, descending.
// This is the ranking step that turns a pile of numbers into an ordered list.
val sortedGrowthRDD = growthRDD.sortBy(_._2._3, ascending = false)

// ACTION first
// Returns the single top record without downloading the whole RDD.
// More efficient than take(1) when you only need the top element.
val topBusiness = sortedGrowthRDD.first()
println(s"\n#1 fastest growing business:")
println(s"  ID:       ${topBusiness._1}")
println(f"  Recent:   ${topBusiness._2._1} check-ins")
println(f"  Baseline: ${topBusiness._2._2} check-ins")
println(f"  Growth:   +${topBusiness._2._3}%.1f%%")
// Insight: the first() action efficiently identifies the single best candidate
// for TripPulse's "hottest place right now" feature

// ACTION take
// Returns top 20 as a local Array for printing.
val top20 = sortedGrowthRDD.take(20)
println("\nTop 20 fastest growing businesses:")
top20.zipWithIndex.foreach { case ((bizId, (recent, baseline, growth)), i) =>
  println(f"  ${i+1}%2d. $bizId  +$growth%.1f%%  (baseline=$baseline → recent=$recent)")
}


// ============================================================================
// ANALYSIS 4: Growing businesses broken down by metro
//
// Question: Which cities have the most momentum, and who is leading in each?
//
// Operations: filter, join (with business metadata), map, groupByKey, collect
// ============================================================================

println("\n" + "="*60)
println("ANALYSIS 4: Growing businesses by metro")
println("="*60)

// TRANSFORMATION filter
// Keep only businesses with positive growth.
// Negative growth means the business is declining — not useful for trending.
val growingOnlyRDD = growthRDD
  .filter { case (_, (_, _, growth)) => growth > 0 }   // filter: positive growth only

// ACTION count
println("Total businesses with positive growth: " + growingOnlyRDD.count())

// TRANSFORMATION join
// Attach business metadata (name, metro, stars) by joining on business_id.
// This is necessary because growthRDD only has IDs, not names or cities.
val growthWithMetaRDD = growingOnlyRDD.join(businessRDD)
// Result: (bizId, ((recent, baseline, growth), (name, metro, stars)))

// TRANSFORMATION map
// Restructure to focus on metro-level analysis.
// (bizId, ((r,b,g), (name, metro, stars))) → (metro, (name, growth, stars))
val metroGrowthRDD = growthWithMetaRDD.map {
  case (_, ((recent, baseline, growth), (name, metro, stars))) =>
    (metro, (name, growth, stars))
}.cache()

// Count growing businesses per metro
val metroCountRDD = metroGrowthRDD
  .map { case (metro, _) => (metro, 1) }
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)

// ACTION collect
// Safe here because there are only 11 metros — tiny result.
// Never use collect() on millions of rows.
val metroBreakdown = metroCountRDD.collect()
println("\nGrowing businesses per metro:")
metroBreakdown.foreach { case (metro, cnt) =>
  println(s"  $metro: $cnt businesses growing")
}

// TRANSFORMATION groupByKey
// Groups all growing businesses by their metro, so we can find the
// top performer within each city.
// Note: groupByKey is used here because the groups are small (11 metros).
// For very large datasets, prefer reduceByKey or aggregateByKey instead.
val topPerMetro = metroGrowthRDD
  .groupByKey()
  .map { case (metro, businesses) =>
    val best = businesses.maxBy(_._2)   // find highest growth within metro
    (metro, best._1, best._2, best._3)  // (metro, name, growth%, stars)
  }
  .sortBy(_._3, ascending = false)

// ACTION take
println("\nTop growing business in each metro:")
topPerMetro.take(11).foreach { case (metro, name, growth, stars) =>
  println(f"  $metro%-15s  $name%-40s  +$growth%.1f%%  ${stars}★")
}
// Insight: this breakdown shows which cities have the most dynamically
// changing business landscape — key for TripPulse's city-level trend alerts


// ============================================================================
// ANALYSIS 5: Summary statistics and save results
//
// Operations: reduce, distinct, foreach, saveAsTextFile
// ============================================================================

println("\n" + "="*60)
println("ANALYSIS 5: Summary statistics and save")
println("="*60)

// ACTION reduce
// Find the single maximum growth rate across the entire dataset.
// reduce() walks the RDD once and collapses it to a single value.
val maxGrowth = growthRDD
  .map { case (_, (_, _, growth)) => growth }
  .reduce((a, b) => math.max(a, b))   // ACTION reduce: find maximum
println(f"Maximum growth rate in the dataset: +$maxGrowth%.1f%%")

// TRANSFORMATION distinct
// Get the unique list of metros that have at least one growing business.
// distinct() removes duplicate values from an RDD.
val growingMetros = metroGrowthRDD.map(_._1).distinct()   // distinct: unique metros

// ACTION count
println("Metros with at least one growing business: " + growingMetros.count())

// ACTION foreach
// Prints each metro directly — side effect action, no return value.
println("\nMetros with growing businesses:")
growingMetros.foreach(metro => println("  " + metro))

// Build a human-readable output line for each growing business
val outputRDD = sortedGrowthRDD
  .join(businessRDD)
  .map { case (bizId, ((recent, baseline, growth), (name, metro, stars))) =>
    f"$growth%+8.1f%% | $metro%-15s | $name%-40s | recent=$recent baseline=$baseline stars=$stars"
  }

// Clean up old output directory if it exists (handles re-runs)
try {
  val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  fs.delete(new org.apache.hadoop.fs.Path(TOP_BIZ_DIR), true)
} catch { case _: Exception => }

// ACTION saveAsTextFile
// Saves the full results as a text file for the report and submission.
// coalesce(1) puts everything in a single part file so it's easy to open.
outputRDD.coalesce(1).saveAsTextFile(TOP_BIZ_DIR)
println("Full ranked results saved to: " + TOP_BIZ_DIR)
// Insight: saveAsTextFile is the RDD equivalent of write() on DataFrames —
// it materializes and persists the computation to disk

// Write text summary for the report
val pw = new PrintWriter(SUMMARY_TXT)
pw.println("TripPulse Phase 3 — Local Trend Discovery (Student 4)")
pw.println("=" * 55)
pw.println(s"Dataset year range:     $minYear – $maxYear")
pw.println(s"Recent window:          $recentStart – $maxYear")
pw.println(s"Baseline window:        $baselineStart – $baselineEnd")
pw.println(s"Min baseline threshold: 10 check-ins")
pw.println()
pw.println(s"Businesses analyzed:           ${growthRDD.count()}")
pw.println(s"Businesses with positive growth: ${growingOnlyRDD.count()}")
pw.println(f"Maximum growth rate:           +$maxGrowth%.1f%%")
pw.println()
pw.println("Top 20 fastest growing businesses:")
top20.zipWithIndex.foreach { case ((bizId, (recent, baseline, growth)), i) =>
  pw.println(f"  ${i+1}%2d. $bizId  +$growth%.1f%%  (baseline=$baseline → recent=$recent)")
}
pw.close()
println("Summary written to: " + SUMMARY_TXT)

println("\n=== Student 4 Phase 3 Complete ===")
