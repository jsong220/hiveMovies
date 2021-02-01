import java.sql._
import scala.io.StdIn.readLine

object Main extends App {
  //println("Hello, World!")
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def sendQuery(initQuery: String, resultQuery: String, headerLabel: String, aggLabel: String, dropQuery: String, connection: Connection) {
    val statement = connection.createStatement()
    var initSet = statement.execute(initQuery)
    var resultSet = statement.executeQuery(resultQuery)

    //grabbing all the columns
    while (resultSet.next) {
      val header = resultSet.getString(headerLabel)
      val aggValue = resultSet.getString(aggLabel)
      println(header + " " + aggValue)
    }
    var endSet = statement.execute(dropQuery)
  }

  var contProg = true
  while (contProg) {
    //start connection to Hive
    //catch error if connection was not made
    var con: Connection = null
    try {
      val connectionString = "jdbc:hive2://localhost:10000/movies"
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      con = DriverManager.getConnection(connectionString, "", "")
    } catch {
      case e: 
        Exception => e.printStackTrace()
        println("Failed to connect to Hive!")
        sys.exit()
    }
    
    //if Hive connection is established correctly..start with intro text
    println("")
    println("Please select a option for analysis..")
    printf(
      "1.Top 10 Most Rated Movies\n2.Top 10 Highest Ratings\n3.Top 10 by Decades\n4.Exit\n\n"
    )
    var userOption = readLine()

    //get valid userinput
    while (
      (toInt(userOption)
        .getOrElse(0) >= 5) || (toInt(userOption).getOrElse(0) <= 0)
    ) {
      println("Please enter a valid number option")
      userOption = readLine()
    }

    userOption.toInt match {
      case 1 =>
        println(" ")
        println("Top 10 Most Rated Movies with rating counts")
        sendQuery("CREATE VIEW IF NOT EXISTS topMovieIds AS SELECT movieId, count(movieid) as ratingCount FROM ratings GROUP BY movieId ORDER BY ratingCount DESC", "SELECT n.title, ratingCount FROM topMovieIds t JOIN movies n ON t.movieId = n.movieId LIMIT 10", "n.title", "ratingCount", "DROP VIEW topmovieids", con)
      case 2 =>
        println(" ")
        println("Top 10 Highest Rated Movies")
        sendQuery("CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieId, avg(rating) as avgRating, COUNT(movieId) as ratingCount FROM ratings GROUP BY movieId ORDER BY avgRating DESC","SELECT n.title as movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 10 LIMIT 10","movie_title","avg_rating","DROP VIEW avgRatings", con)
      case 3 =>
        println(" ")
        println("Top 10 Highest Rated Movies by Different Decades")
        println("From 1995 To 2000")
        sendQuery("CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieid,avg(rating) AS avgRating, COUNT(movieId) as ratingCount FROM ratings WHERE t < 946684800 GROUP BY movieid ORDER BY avgRating DESC", "SELECT n.title AS movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 100 LIMIT 10", "movie_title", "avg_rating", "DROP VIEW avgRatings", con)

        println(" ")
        println("From 2000 To 2010")
        sendQuery("CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieid,avg(rating) AS avgRating, COUNT(movieId) as ratingCount FROM ratings WHERE t < 1262304000 GROUP BY movieid ORDER BY avgRating DESC", "SELECT n.title AS movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 100 LIMIT 10", "movie_title", "avg_rating", "DROP VIEW avgRatings", con)

        println(" ")
        println("From 2010 To 2018")
        sendQuery("CREATE VIEW IF NOT EXISTS avgRatings AS SELECT movieid,avg(rating) AS avgRating, COUNT(movieId) as ratingCount FROM ratings WHERE t < 1577836800 GROUP BY movieid ORDER BY avgRating DESC", "SELECT n.title AS movie_title, cast(avgRating AS DECIMAL(10,2)) AS avg_rating FROM avgRatings t JOIN movies n ON t.movieid = n.movieId WHERE ratingCount > 100 LIMIT 10", "movie_title", "avg_rating", "DROP VIEW avgRatings", con)

      case 4 =>
        //close connection when exiting
        con.close()
        contProg = false;
      case _ => println("you should not be here..")
    }
  }
}