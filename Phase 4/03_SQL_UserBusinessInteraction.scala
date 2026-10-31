import java.io.File
import org.apache.spark.sql.functions._
DOWNLOADS: String = C:/Users/demoh/Downloads
containsParquet: (dir: java.io.File)Boolean
findDatasetDirs: (root: java.io.File, targetName: String)Seq[java.io.File]
chooseDataset: (targetName: String, preferred: Seq[String])String
REVIEW_PATH: String = C:/Users/demoh/Downloads/sample4/review_features
USER_PATH: String = C:/Users/demoh/Downloads/user_features/user_features
BUSINESS_PATH: String = C:/Users/demoh/Downloads/business_features/business_features
review_features   = C:/Users/demoh/Downloads/sample4/review_features
user_features     = C:/Users/demoh/Downloads/user_features/user_features
business_features = C:/Users/demoh/Downloads/business_features/business_features
reviewSQL: org.apache.spark.sql.DataFrame = [user_id: string, business_id: string ... 1 more field]
userSQL: org.apache.spark.sql.DataFrame = [user_id: string, review_count: bigint]
26/10/31 16:42:06 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
businessSQL: org.apache.spark.sql.DataFrame = [business_id: string, metro: string ... 1 more field]
Temporary views created successfully.
result: org.apache.spark.sql.DataFrame = [metro: string, category: string ... 2 more fields]

================================================================================
TOP 20 METRO-CATEGORY COMBINATIONS REVIEWED BY ACTIVE USERS
================================================================================
+-------------+----------------------+-------------------+----------+
|metro        |category              |active_user_reviews|avg_rating|
+-------------+----------------------+-------------------+----------+
|Philadelphia |Restaurants           |273867             |3.82      |
|Tampa        |Restaurants           |175623             |3.89      |
|New Orleans  |Restaurants           |123040             |3.96      |
|Philadelphia |Food                  |104580             |3.91      |
|Nashville    |Restaurants           |93194              |3.92      |
|St. Louis    |Restaurants           |81184              |3.89      |
|Indianapolis |Restaurants           |78058              |3.94      |
|Philadelphia |Nightlife             |76442              |3.76      |
|Philadelphia |Bars                  |72796              |3.75      |
|Tampa        |Food                  |71786              |4.01      |
|Tampa        |Nightlife             |61567              |3.87      |
|Tampa        |Bars                  |59805              |3.87      |
|Tucson       |Restaurants           |59648              |3.8       |
|Philadelphia |American (New)        |58188              |3.8       |
|Philadelphia |American (Traditional)|53707              |3.62      |
|Reno         |Restaurants           |53525              |3.81      |
|Santa Barbara|Restaurants           |50529              |4.01      |
|New Orleans  |Food                  |48725              |4.01      |
|Philadelphia |Breakfast & Brunch    |47564              |3.89      |
|Philadelphia |Sandwiches            |46487              |3.82      |
+-------------+----------------------+-------------------+----------+

activeUserCount: Long = 357985
High-activity users (review_count >= 20): 357985
OUTPUT_DIR: String = C:/Users/demoh/Downloads/SQL_Student3_UserBusinessInteraction
Result saved to: C:/Users/demoh/Downloads/SQL_Student3_UserBusinessInteraction
=== Student 3 SQL User-Business Interaction complete ===