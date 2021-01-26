import java.sql._
import scala.io.StdIn.readLine

object Main extends App {
  //println("Hello, World!")
  //TODO: set up user interface..input and prompts
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  var contProg = true
  while (contProg) {
    println("")
    println("Please select a option for analysis..")
    printf(
      "1.Top 10 Most Rated Movies\n2.Top 10 Highest Ratings\n3.Top 10 by Decades\n4.Exit\n\n"
    )
    var userOption = readLine()

    while (
      (toInt(userOption)
        .getOrElse(0) >= 5) || (toInt(userOption).getOrElse(0) <= 0)
    ) {
      println("Please enter a valid number option")
      userOption = readLine()
    }

    userOption.toInt match {
      case 1 =>
        var con: Connection = null
        try {
          //set up connection
          // val connectionString = "jdbc:hive2://localhost:10000/default"
          //if not using default database
          val connectionString = "jdbc:hive2://localhost:10000/movies"
          Class.forName("org.apache.hive.jdbc.HiveDriver")
          con = DriverManager.getConnection(connectionString, "", "")

          //send query
          val statement = con.createStatement()
          var initSet = statement.execute(
            "CREATE VIEW IF NOT EXISTS topMovieIds AS SELECT movieId, count(movieid) as ratingCount FROM ratings GROUP BY movieId ORDER BY ratingCount DESC"
          )
          var resultSet = statement.executeQuery(
            "SELECT n.title, ratingCount FROM topMovieIds t JOIN movies n ON t.movieId = n.movieId LIMIT 10"
          )

          //grabbing all the columns
          while (resultSet.next) {
            val title = resultSet.getString("n.title")
            val ratingCount = resultSet.getString("ratingCount")
            println(title + " " + ratingCount)
          }
          var endSet = statement.execute("DROP VIEW topmovieids")

        } catch {
          case e: Exception => e.printStackTrace()
        }
        con.close()
      case 2 =>
        var con: Connection = null
        try {
          //set up connection
          // val connectionString = "jdbc:hive2://localhost:10000/default"
          //if not using default database
          val connectionString = "jdbc:hive2://localhost:10000/movies"
          Class.forName("org.apache.hive.jdbc.HiveDriver")
          con = DriverManager.getConnection(connectionString, "", "")

          //send query
          val statement = con.createStatement()
          var initSet = statement.execute(
            "CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieId, avg(rating) as avgRating, COUNT(movieId) as ratingCount FROM ratings GROUP BY movieId ORDER BY avgRating DESC"
          )
          var resultSet = statement.executeQuery(
            "SELECT n.title as movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 10 LIMIT 10"
          )

          //grabbing all the columns
          while (resultSet.next) {
            val movieTitle = resultSet.getString("movie_title")
            val avgRating = resultSet.getString("avg_rating")
            println(movieTitle + " " + avgRating)
          }
          var endSet = statement.execute("DROP VIEW avgRatings")

        } catch {
          case e: Exception => e.printStackTrace()
        }
        con.close()
      case 3 =>
        var con: Connection = null
        try {
          //set up connection
          // val connectionString = "jdbc:hive2://localhost:10000/default"
          //if not using default database
          val connectionString = "jdbc:hive2://localhost:10000/movies"
          Class.forName("org.apache.hive.jdbc.HiveDriver")
          con = DriverManager.getConnection(connectionString, "", "")

          //send query
          val statement = con.createStatement()
          var initSet = statement.execute(
            "CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieid,avg(rating) AS avgRating, COUNT(movieId) as ratingCount FROM ratings WHERE t < 946684800 GROUP BY movieid ORDER BY avgRating DESC"
          )
          var resultSet = statement.executeQuery(
            "SELECT n.title AS movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 100 LIMIT 10"
          )
          //1995 to 2000
          println("From 1995 To 2000")
          //grabbing all the columns
          while (resultSet.next) {
            val movieTitle = resultSet.getString("movie_title")
            val avgRating = resultSet.getString("avg_rating")
            println(movieTitle + " " + avgRating)
          }
          var endSet = statement.execute("DROP VIEW avgRatings")

          //2000 to 2010
          initSet = statement.execute(
            "CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieid,avg(rating) AS avgRating, COUNT(movieId) as ratingCount FROM ratings WHERE t < 1262304000 GROUP BY movieid ORDER BY avgRating DESC"
          )
          resultSet = statement.executeQuery(
            "SELECT n.title AS movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 100 LIMIT 10"
          )
          println(" ")
          println("From 2000 To 2010")
          //grabbing all the columns
          while (resultSet.next) {
            val movieTitle = resultSet.getString("movie_title")
            val avgRating = resultSet.getString("avg_rating")
            println(movieTitle + " " + avgRating)
          }
          endSet = statement.execute("DROP VIEW avgRatings")

          //2010 to 2020
          initSet = statement.execute(
            "CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieid,avg(rating) AS avgRating, COUNT(movieId) as ratingCount FROM ratings WHERE t < 1577836800 GROUP BY movieid ORDER BY avgRating DESC"
          )
          resultSet = statement.executeQuery(
            "SELECT n.title AS movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 100 LIMIT 10"
          )
          println(" ")
          println("From 2010 To 2018")
          //grabbing all the columns
          while (resultSet.next) {
            val movieTitle = resultSet.getString("movie_title")
            val avgRating = resultSet.getString("avg_rating")
            println(movieTitle + " " + avgRating)
          }
          endSet = statement.execute("DROP VIEW avgRatings")

        } catch {
          case e: Exception => e.printStackTrace()
        }
        con.close()
      case 4 => contProg = false;
      case _ => println("you should not be here..")
    }
  }
}

