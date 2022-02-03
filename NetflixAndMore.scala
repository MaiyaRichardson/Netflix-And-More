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
        
        

        println("")
        //val users1 = Users(connection)
        
        // Method to check login credentials
        /*val adminCheck = login(connection)
        if (adminCheck) {
            println("Welcome Admin!" + "\n" + "Here is a list of all your functions:" + "\n")
            
            val admin1 = adminAccess(connection)
        
        } else {
            println("Welcome User! Loading in data...")
        
            
        }
        */
      

        // Run method to insert Covid data. Only needs to be ran initially, then table data1 will be persisted.
        //top10NetflixMoves(hiveCtx)
        //top10DeathRates(hiveCtx)
        //top10PrimeVideoMovies(hiveCtx)
        //top10PrimeVideoMovies(hiveCtx)
        top10NetflixByAge(hiveCtx)

        /*
        * Here is where I would ask the user for input on what queries they would like to run, as well as
        * method calls to run those queries. An example is below, top10DeathRates(hiveCtx) 
        * 
        */

        //top10DeathRates(hiveCtx)

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
    

    /*def adminAccess(connection: Connection): Boolean = {
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
                    var user_ID = scanner.next().toString()uijn
                    println()

                    val result = statement.executeUpdate("DELETE FROM Users WHERE user_ID = '"+user_ID+"');")

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
    */
    /*def Users(connection: Connection): Boolean = {
        
        var scanner = new Scanner(System.in)
        val statement = connection.createStatement()
        
        var create = 1
        var update = 2
        var updateP = 3

        scanner.useDelimiter(System.lineSeparator())

        while(true){
            try {
            println("Here is a list of User functions: " + "\n\n Press 1 if you want to create an account: " + "\n Press 2 if you want to update your username: " + "\n Press 3 if you would like to update your password: ")
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
    */
    /*def insertCovidData(hiveCtx:HiveContext): Unit = {
                //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/covid_19_data.txt' OVERWRITE INTO TABLE data1")
        //hiveCtx.sql("INSERT INTO data1 VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

        // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
        // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can 
        // then be 
        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/MoviesOnStreamingPlatforms.csv")
        output.limit(15).show() // Prints out the first 15 lines of the dataframe

        // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 

        // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
        // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this 
        // code as well as the creation of output will not be necessary.
        //output.createOrReplaceTempView("temp_data")
        //hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1 (SNo INT, ObservationDate STRING, Province_State STRING, Country_Region STRING, LastUpdate STRING, Confirmed INT, Deaths INT, Recovered INT)")
        //hiveCtx.sql("INSERT INTO data1 SELECT * FROM temp_data")
        
        // To query the data1 table. When we make a query, the result set ius stored using a dataframe. In order to print to the console, 
        // we can use the .show() method.
        //val summary = hiveCtx.sql("SELECT * FROM data1 LIMIT 10")
        //summary.show()
    }
    */
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
        result2.write.csv("results/top10NetflixMoviesFromYear2015")


    }

    def top10DeathRates(hiveCtx:HiveContext): Unit = {
      
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

    def top10PrimeVideoMovies(hiveCtx:HiveContext): Unit = {
        hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        hiveCtx.sql("SET hive.enforce.bucketing=false")
        hiveCtx.sql("SET hive.enforce.sorting=false")

        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/MoviesOnStreamingPlatforms.csv")
            

        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS PrimeVideo2 (_co INT, ID INT, Title STRING, Year INT, Age STRING, RottenTomatoes STRING, Netflix STRING, Hulu INT, PrimeVideo INT, Disney INT, Type INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u003b' ")
        hiveCtx.sql("INSERT INTO PrimeVideo2 SELECT * FROM temp_data")
        
        //val result1 = hiveCtx.sql("CREATE VIEW PrimeVideo4 AS SELECT Title, Year, Age, PrimeVideo, CAST(regexp_replace(RottenTomatoes, '/100', '') AS int) AS RottenTomatoes FROM PrimeVideo2")
        
        val result2 = hiveCtx.sql("SELECT Title, Year, Age, PrimeVideo, RottenTomatoes FROM PrimeVideo4 WHERE PrimeVideo = 1 AND RottenTomatoes >= 85 AND Year >= 2015 LIMIT 10")
        result2.show()
        result2.write.csv("results/top10PrimeVideosFromYear2015")
    }

    def top10NetflixByAge(hiveCtx:HiveContext): Unit = {
        hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        hiveCtx.sql("SET hive.enforce.bucketing=false")
        hiveCtx.sql("SET hive.enforce.sorting=false")

        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/MoviesOnStreamingPlatforms.csv")
            

        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS NetflixAge (_co INT, ID INT, Title STRING, Year INT, Age STRING, RottenTomatoes STRING, Netflix STRING, Hulu INT, PrimeVideo INT, Disney INT, Type INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u003b' ")
        hiveCtx.sql("INSERT INTO NetflixAge SELECT * FROM temp_data")
        
        val result1 = hiveCtx.sql("CREATE VIEW NetAge AS SELECT Title, Year, Age, Netflix, CAST(regexp_replace(RottenTomatoes, '/100', '') AS int) AS RottenTomatoes FROM NetflixAge")
        val result2 = hiveCtx.sql("SELECT Title, Year, Age, Netflix, RottenTomatoes FROM NetAge WHERE Netflix = 1 AND RottenTomatoes >= 85 AND Year >= 2015 AND Age = '18+' LIMIT 10")
        result2.show()
    
        result2.write.csv("results/top10NetflixFromYear2015ByAge")
    }
}

