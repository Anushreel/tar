import org.apache.spark.{SparkConf, SparkContext}
object SumWithFoldAggregate {
def main(args: Array[String]): Unit = {
val conf = new SparkConf().setAppName("SumWithFoldAggregate").setMaster("local[*]")
val sc = new SparkContext(conf)
val data = List(1, 2, 3, 4, 5)
val rc = sc.parallelize(data)
// Using fold
val updatedFold = rc.fold(0)((a, b) => a + b + 100)
// Using aggregate
val updatedAggr = rc.aggregate(0)((a, b) => a + b + 100, (a1, a2) => a1 + a2)
println("Using fold: " + updatedFold)
println("Using aggregate: " + updatedAggr)
sc.stop()
}
}
