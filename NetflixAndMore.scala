import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import org.apache.hadoop.hive.ql.metadata.Hive


object NetflixAndMore {
    def main(args: Array[String]): Unit = {
        // This block of code is all necessary for spark/hive/hadoop
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("NetflixAndMore")    // Change to whatever app name you want
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

        //This block to connect to mySQL
        val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/netflixandmore" // Modify for whatever port you are running your DB on
        val username = "root"
        val password = "asdfgh3" // Update to include your password
        var connection:Connection = null

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
       
        var scanner = new Scanner(System.in)
        
        scanner.useDelimiter(System.lineSeparator())

        println("")
        //val users1 = Users(connection)
        
        // Method to check login credentials
        val adminCheck = login(connection)
        try {

            val admin = 1
            val userentry = 2

            println("(1) For Admin and User login" + "\n(2) For new user entry")
            println()
            var input = scanner.next().trim().toInt

            if (input == admin) {
                
                if (adminCheck){

                    println("Welcome Admin!" + "\n" + "Here is a list of all your functions:" + "\n")
                
                    val admin1 = adminAccess(connection)
                
                }
                else{ 
                    println("Welcome User! Loading in data...")
                    
                    val net1 = 1
                    val net2 = 2
                    val prm = 3
                    val usr = 5
                    val prmAge = 4
                    println("(1) to view top 10 Netflix Movies starting from the year 2015" + "\n(2) to view top 10 Netflix Movies rated 'R' starting from the year 2015" + "\n(3) to view top 10 Prime Movies starting from the year 2015"  + "\n(4) to view top 10 Prime Movies rated 'R' starting from the year 2015" + "\n(5) return to Users menu")
                    println()
                    var input = scanner.next().trim().toInt
                    if (input == net1){
                        top10NetflixMoves(hiveCtx)
                        main(args: Array[String])
                    }
                    if (input == net2){
                        top10NetflixByAge(hiveCtx)
                        main(args: Array[String])
                    }
                    if (input == prm){
                        top10PrimeVideoMovies(hiveCtx)
                        main(args: Array[String])
                    }
                    if (input == prmAge){
                        top10PrimeVideoMoviesAge(hiveCtx)
                        main(args: Array[String])
                    }
                    if (input == usr){
                        val users1 = Users(connection)
                        main(args: Array[String])
                    }
                    
                }
                
            }
            if (input == userentry){
                val users1 = Users(connection)
            }
            
        
        }catch {
            case bue: BadUserEntryException => println("You did not enter in a number. Please try again")
            main(args: Array[String])
        }
        
            

        sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
    }

    // This method checks to see if a user-inputted username/password combo is part of a mySQL table.
    // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
    def login(connection: Connection): Boolean = {
        
        while (true) {
            val statement = connection.createStatement()
            val statement2 = connection.createStatement()

            println("Enter username: ")
            var scanner = new Scanner(System.in)
            var username = scanner.nextLine().trim()

            println("Enter password: ")
            var password = scanner.nextLine().trim()

            
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM AdminUser WHERE admin_name='"+username+"' AND admin_password='"+password+"';")
            while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    return true;
                }
            }
            
            

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM Users WHERE username='"+username+"' AND user_password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                    return false;
                }
            }

            println("Username/password combo not found. Try again!")
        }
        return false
    }
    

    def adminAccess(connection: Connection): Boolean = {
        var scanner = new Scanner(System.in)
        val statement = connection.createStatement()
        scanner.useDelimiter(System.lineSeparator())

        var delete = 1

        while (true){
            try {
                println("As an admin you can delete a user." + "\nTo delete a user, press 1:")
                println()
                var input = scanner.next().trim().toInt

                if (input == delete){
                    println()
                    println("To delete an account, enter in their ID")
                    var user_ID = scanner.next().toString().toInt
                    println()

                    val result = statement.executeUpdate("DELETE FROM Users WHERE user_ID = '"+user_ID+"';")

                    return true
                }
                else 
                    throw new BadUserEntryException
            }catch {
                case bue: BadUserEntryException => println ("You did not enter in a number. Please try again")
            }
        }
        return false
    }
    
    def Users(connection: Connection): Boolean = {
        
        var scanner = new Scanner(System.in)
        val statement = connection.createStatement()
        
        var create = 1
        var update = 2
        var updateP = 3
        var menu = 4

        scanner.useDelimiter(System.lineSeparator())

        while(true){
            try {
            println("Here is a list of User functions: " + "\n\n(1) if you want to create an account: " + "\n(2) if you want to update your username: " + "\n(3) if you would like to update your password: " + "\n(4) if you would like to return to the main menu")
            println()
            var input = scanner.next().trim().toInt

            if(input == create){
                
                
                println("Please enter in an username: ")
                var username = scanner.next().toString()
                println()

                println("Please enter in your email address: ")
                var user_emailAddress = scanner.next().toString()
                println()

                println("Please enter in a password: ")
                var user_password = scanner.next().toString()
                println()
                
                val results = statement.executeUpdate("INSERT INTO Users (username, user_password, user_emailAddress) VALUES ('"+username+"' , '"+user_password+"', '"+user_emailAddress+"');")
                
                println("Welcome " + username)
                return true

            }
            if(input == update){

                println("To update your username, please enter in your email address: " + "\n")
                var user_emailAddress = scanner.next().toString()
                println()

                println("Please enter in your new username: " + "\n")
                var username = scanner.next().toString()
                println()

                val result = statement.executeUpdate("UPDATE Users SET username = '"+username+"' WHERE user_emailAddress = '"+user_emailAddress+"';")

                
                println("Your username has been changed to " + username + ".")

                return true
            }
            if(input == updateP){
                println("To update your password, please enter in your email address: " + "\n")
                var user_emailAddress = scanner.next().toString()
                println()

                println("Please enter in your new password: " + "\n")
                var user_password = scanner.next().toString()
                println()

                val result1 = statement.executeUpdate("Update Users SET user_password = '"+user_password+"' WHERE user_emailAddress = '"+user_emailAddress+"';")

                println("Your password has been changed successfully. A confirmation email has been sent to: " + "\n" + user_emailAddress)

                return true
            }
            else 
                throw new BadUserEntryException
            }catch {
                case bue: BadUserEntryException => println("You did not enter in a number. Please try again")
            }
        }
        return false
        
    }

    
   
    def top10NetflixMoves(hiveCtx:HiveContext): Unit = {
        

        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/MoviesOnStreamingPlatforms.csv")
        //output.limit(15).show()
        
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS NetflixData (_co INT, ID INT, Title STRING, Year INT, Age STRING, RottenTomatoes STRING, Netflix STRING, Hulu INT, PrimeVideo INT, Disney INT, Type INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u003b' ")
        hiveCtx.sql("INSERT INTO NetflixData SELECT * FROM temp_data")
        
        //val result1 = hiveCtx.sql("CREATE VIEW RottenTomatoes1 AS SELECT Title, Year, Age, Netflix, CAST(regexp_replace(RottenTomatoes, '/100', '') AS int) AS RottenTomatoes FROM NetflixData")
        val result2 = hiveCtx.sql("SELECT Title, Year, Age, Netflix, RottenTomatoes FROM RottenTomatoes1 WHERE Netflix = 1 AND RottenTomatoes >= 85 AND Year >= 2015 LIMIT 10")
        result2.show()
        //result2.write.csv("results/top10NetflixMoviesFromYear2015")


    }

    /*def PartitionedData(hiveCtx:HiveContext): Unit = {
      
        hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        hiveCtx.sql("SET hive.enforce.bucketing=false")
        hiveCtx.sql("SET hive.enforce.sorting=false")

        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/MoviesOnStreamingPlatforms.csv")
            

        output.createOrReplaceTempView("temp_data")
        
        //
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS NetflixData1 (_co INT, ID INT, Title STRING, Year INT, Age STRING, RottenTomatoes STRING, Netflix STRING, Hulu INT, PrimeVideo INT, Disney INT, Type INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u003b' stored as textfile")
        hiveCtx.sql("ALTER TABLE NetflixData1 SET TBLPROPERTIES (\"skip.header.line.count\"=\"1\")")
        hiveCtx.sql("INSERT INTO NetflixData1 SELECT * FROM temp_data")

        hiveCtx.sql("CREATE TABLE IF NOT EXISTS NetflixPart (_co INT, ID INT, Title STRING, Year INT, Age STRING, Netflix STRING, Hulu INT, PrimeVideo INT, Disney INT, Type INT) PARTITIONED BY (RottenTomatoes STRING) CLUSTERED BY (Netflix) INTO 10 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u003b' stored as textfile")
        hiveCtx.sql("INSERT OVERWRITE TABLE NetflixPart SELECT _co, ID, Title, Year, Age, Netflix, Hulu, PrimeVideo, Disney, Type, RottenTomatoes FROM NetflixData1")
        val result = hiveCtx.sql("SELECT * FROM NetflixPart LIMIT 20")
        result.show()
        //result.write.csv("results/top10DeathRates")


    }
    */

    def top10PrimeVideoMovies(hiveCtx:HiveContext): Unit = {
        
        
        val result2 = hiveCtx.sql("SELECT Title, Year, Age, PrimeVideo, RottenTomatoes FROM NetflixData WHERE PrimeVideo = 1 AND RottenTomatoes >= 85 AND Year >= 2015 LIMIT 10")
        result2.show()
        //result2.write.csv("results/top10PrimeVideosFromYear2015")
    }

    def top10PrimeVideoMoviesAge(hiveCtx:HiveContext): Unit = {
        
        //val result1 = hiveCtx.sql("CREATE VIEW PrimeVideo6 AS SELECT Title, Year, Age, PrimeVideo, CAST(regexp_replace(RottenTomatoes, '/100', '') AS int) AS RottenTomatoes FROM NetflixData")
        
        val result2 = hiveCtx.sql("SELECT Title, Year, Age, PrimeVideo, RottenTomatoes FROM PrimeVideo6 WHERE PrimeVideo = 1 AND RottenTomatoes >= 70 AND Year >= 2015 AND Age = '18+' LIMIT 10")
        result2.show()
        result2.write.csv("results/top10PrimeVideosFromYear2015By18New")
    }

    def top10NetflixByAge(hiveCtx:HiveContext): Unit = {
        
        //val result1 = hiveCtx.sql("CREATE VIEW NetAge AS SELECT Title, Year, Age, Netflix, CAST(regexp_replace(RottenTomatoes, '/100', '') AS int) AS RottenTomatoes FROM NetflixData")
        val result2 = hiveCtx.sql("SELECT Title, Year, Age, Netflix, RottenTomatoes FROM NetAge WHERE Netflix = 1 AND RottenTomatoes >= 85 AND Year >= 2015 AND Age = '18+' LIMIT 10")
        result2.show()
    
        //result2.write.csv("results/top10NetflixFromYear2015ByAge")
    }
}

