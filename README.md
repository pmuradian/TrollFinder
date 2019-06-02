TrollFinder is a small program in spark, which reads input of format (userID, movieID, rating) from a file (including hdfs)
and finds potential spammers who rate movies 1 or 10 to change their rating.

To run use `sbt run`, this will run `main` function of `Object Main` in src/main/scala/TrollFinder.scala

Random input data will be generated and spammers will be filtered out and printed.