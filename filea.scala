package com.bnpparibasfortis.datahub.fileanalyzer

import java.io.FileNotFoundException
import java.nio.charset.{CharacterCodingException, MalformedInputException}

import com.bnpparibasfortis.datahub.fileanalyzer.util.SparkSessionWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Try}

class FileAnalyzerException(s: String) extends Exception(s) {}

object FileAnalyzer extends java.io.Serializable with SparkSessionWrapper {

    import spark.implicits._

    @throws(classOf[FileAnalyzerException])
    var validationResults = new ListBuffer[Boolean]()

    def main(args: Array[String]): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark.sparkContext
        val inputFilePath = args(0)
        val schemaPath = args(1)
        val delimiter = args(2)
        checkFileExistence(inputFilePath, schemaPath, sc)
        val fileAnalyzerOutput = execute(inputFilePath, schemaPath, delimiter, spark, sc)
        val checkOutput = fileAnalyzerOutput.contains(false)
        if (checkOutput == true) {
            throw new FileAnalyzerException("The File Analyzer job has completed successfully but with failure in few test. Please check the logs for details on the test that have failed.")
        } else {
            println("File Analyzer completed successfully without any mismatch between the Input File and JSON schema.")
        }
    }

    /** Executes all the validations
     *
     * @param inputFilePath       Input file from source.
     * @param powerDesignerSchema Json schema of input.
     * @param delimiter           input file delimiter.
     * @param spark               spark session.
     * @param sc                  spark context.
     **/
    def execute(inputFilePath: String, powerDesignerSchema: String, delimiter: String, spark: SparkSession, sc: SparkContext): ListBuffer[Boolean] = {
        val sqlContext: SQLContext = spark.sqlContext
        var replacingQuotedColumnNames = spark.emptyDataFrame
        var replacingNonQuotedColumnNames = spark.emptyDataFrame
        var inputFileCount: java.lang.Long = null

        val jsonSchema = readJson(powerDesignerSchema, sqlContext, sc)
        val sourceFileDFWithQuotes = readInputFileWithQuotedValues(inputFilePath, delimiter, spark)
        replacingQuotedColumnNames = replaceSpecialCharacterColumns(sourceFileDFWithQuotes)

        val sourceFileDFWithoutQuotes = readInputFileWithoutQuotedValues(inputFilePath, delimiter, spark)
        replacingNonQuotedColumnNames = replaceSpecialCharacterColumns(sourceFileDFWithoutQuotes)
        inputFileCount = replacingNonQuotedColumnNames.count()

        /** The function validateUsingStreamReader reads the input file using input stream and validates the encoding and delimiters in file. **/
        validateUsingStreamReader(inputFilePath, delimiter)

        try {
            /** Validates the headers of the input file */
            validateHeaders(replacingQuotedColumnNames, jsonSchema)

            /** Validates the quoting of the input file */
            validateFileQuoting(replacingQuotedColumnNames, jsonSchema)

            /** Validates the format of the date fields in the input file */
            validateDateFormats(replacingQuotedColumnNames, jsonSchema)

            /** Verifies if the column marked as mandatory field does not have null values. */
            validateMandatoryFields(replacingNonQuotedColumnNames, jsonSchema, inputFileCount)

            /** Validates the data types between the input file and power designer schema. */
            validateDataTypes(replacingNonQuotedColumnNames, jsonSchema, inputFileCount)

            /** Verifies if there are additional columns generated in the input file than the power designer schema. */
            validateAdditionalColumns(replacingNonQuotedColumnNames, jsonSchema)

            /** Verifies if there are NULL values present in input file. */
            validateNullValues(replacingNonQuotedColumnNames)

            /** Verifies if the primary key or key combinations have duplicates or null values in input file. */
            validatePrimaryKeyColumns(replacingNonQuotedColumnNames, jsonSchema)
        }
        catch {
            case indexOutOfBoundsException: IndexOutOfBoundsException => {
                println("Column Mismatch between input file and schema - There are less fields in input file that in power designer schema.")
            }
            case exception: AnalysisException => {
                println("Column Mismatch between input file and schema - There are more fields in input file that in power designer schema.")
            }
        }
        validationResults
    }

    /** The function readJson reads the json file to a data frame.
     *
     * @param powerDesignerSchema Input file from source.
     * @param sqlContext          SQL Context.
     * @param sc                  Spark context.
     * */
    def readJson(powerDesignerSchema: String, sqlContext: SQLContext, sc: SparkContext): DataFrame = {
        try {
            /** Read json schema to a data frame */
            val json = sqlContext.read.option("multiLine", "true").json(sc.wholeTextFiles(powerDesignerSchema).values)
            val jsonFlat = json.withColumn("Attributes_flat", explode($"Attributes")).drop("Attributes")
            val jasonDF = jsonFlat.select($"entity-code", $"Attributes_flat.*")
            jasonDF
        }
        catch {
            case exception: AnalysisException => {
                println("Issue with Json file. Check if there are issues in JSON file.")
                throw exception
            }
        }
    }

    /** The function readInputFileWithQuotedValues reads the input file to a data frame with quoted values.
     *
     * @param inputFilePath Input file from source.
     * @param delimiter     Input file delimiter.
     * @param spark         Spark session.
     * */
    def readInputFileWithQuotedValues(inputFilePath: String, delimiter: String, spark: SparkSession): DataFrame = {
        /** Read input file to a data frame with quoted values */
        val inputDFWithQuotedValues = spark.read
            .option("delimiter", delimiter)
            .option("header", true)
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("quote", "")
            .csv(inputFilePath)

        inputDFWithQuotedValues
    }

    /** The function readInputFileWithoutQuotedValues reads the input file to a data frame without quoted values.
     *
     * @param inputFilePath Input file from source.
     * @param delimiter     Input file delimiter.
     * @param spark         Spark session.
     * */
    def readInputFileWithoutQuotedValues(inputFilePath: String, delimiter: String, spark: SparkSession): DataFrame = {
        /** Read input file to a data frame without quoted values */
        val inputDFWithoutQuotedValues = spark.read
            .option("delimiter", delimiter)
            .option("header", true)
            .option("inferSchema", "true")
            .option("multiLine", true)
            .csv(inputFilePath)

        inputDFWithoutQuotedValues
    }

    /** The function validateUsingStreamReader reads the input file using input stream and validates the encoding and delimiters in file.
     *
     * @param inputFilePath Input file from source.
     * @param delimiter     the input file delimiter.
     * */
    def validateUsingStreamReader(inputFilePath: String, delimiter: String) = {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        var stream: FSDataInputStream = null
        var source: BufferedSource = null

        /** Read input file to reader for processing */
        stream = fs.open(new Path(inputFilePath))
        source = Source.fromInputStream(stream, "UTF-8")

        /** Validates the encoding of the file */
        validateFileEncoding(source)

        try {
            /** Validates the delimiter of the file */
            validateFileDelimiter(source, delimiter)
        }
        catch {
            case e: MalformedInputException => {
                println("Verify if the input file is of UTF-8 encoding.")
            }
        }
        finally {
            stream.close()
        }

    }

    /** The function validateFileEncoding validates the Encoding of the file.
     *
     * @param source data input stream
     * */
    def validateFileEncoding(source: BufferedSource): Unit = {
        println("============== 1. Check file encoding ==============")
        try {
            val data = source.getLines().take(1).toList
            validationResults += true
            println("The file has a valid utf-8 encoding - PASS.")
        } catch {
            case e: CharacterCodingException =>
                validationResults += false
                println("The file is not in utf-8 - FAIL")
            case e: MalformedInputException =>
                validationResults += false
                println("The file is not in utf-8 - FAIL")
                throw e
        }
    }

    /** The function validateDelimiter validates the Delimiter in the  file.
     *
     * @param source    data input buffer reader
     * @param delimiter input file delimiter
     **/
    def validateFileDelimiter(source: BufferedSource, delimiter: String): Unit = {
        /** Validates the delimiter of the file */
        val data = source.getLines().take(1).toList
        println("============== 2. Check delimiter ==============")
        if (data.toString().contains(delimiter)) {
            validationResults += true
            println("The file is semicolon delimited - PASS")
        }
        else {
            validationResults += false
            println("The file is not semicolon delimited - FAIL")
        }
    }

    /** The function validateFileEncoding validates the Encoding of the file.
     *
     * @param sourceFile Data frame with source data.
     * */
    def replaceSpecialCharacterColumns(sourceFile: DataFrame): DataFrame = {
        var replacedColumnNames = sourceFile.select(
            sourceFile.columns
                .map(colName => col(s"`${colName}`").as(colName.replaceAll("#", ""))): _*
        )
        replacedColumnNames
    }

    /** The function validateHeaders validates the headers of the input file.
     *
     * @param sourceFile a data frame with the contents of the input file.
     * @param schema     Json schema of the input used
     * */
    def validateHeaders(sourceFile: DataFrame, schema: DataFrame): Unit = {
        println("============== 3. Check file headers ==============")
        val schemaColumns = schema.select("attrib-code").map(r => r.getString(0)).collect.toList
        val inputFileColumns = sourceFile.columns.toList
        var headerTest: Boolean = true

        for (j <- 0 to schemaColumns.length - 1) {
            if (schemaColumns(j).toUpperCase != inputFileColumns(j).toUpperCase) {
                headerTest = false
                println("Header " + schemaColumns(j) + " has wrong name or ordinal - FAIL")
            }
        }
        if (headerTest == true) {
            validationResults += true
            println("Header names & ordinals are correct - PASS")
        }
        else {
            validationResults += false
        }
    }

    /** The function validateFileQuoting validates the quoting of individual values in the input file.
     *
     * @param sourceFile a data frame with the contents of the input file.
     * @param schema     Json schema of the input used
     * */
    def validateFileQuoting(sourceFile: DataFrame, schema: DataFrame): Unit = {
        println("============== 4. Check if the file is consistently quoted ==============")
        val charColumns = schema.select("attrib-code").where(schema("attrib-datatype") !== "number").map(r => r.getString(0)).collect.toList
        val quotedColumns = sourceFile.select(charColumns.map(col): _*)
        var columnWithIssues = new ListBuffer[String]()
        var testStatus: Boolean = true
        for (i <- 0 to quotedColumns.columns.size - 1) {
            val wronglyQuotedcolumns = quotedColumns.filter(!(quotedColumns(quotedColumns.columns(i)).startsWith("\"") && quotedColumns(quotedColumns.columns(i)).endsWith("\"")))
            val countWronglyQuotedcolumns = wronglyQuotedcolumns.count()
            if (countWronglyQuotedcolumns > 0) {
                val testOutput = wronglyQuotedcolumns.select(wronglyQuotedcolumns.columns(i)).first.getString(0)
                println("There are inconsistencies in quoting for the column - " + quotedColumns.columns(i).toString + " for the value " + testOutput + " - FAIL")
                columnWithIssues += quotedColumns.columns(i)
                testStatus = false
            }
        }
        if (testStatus == true) {
            validationResults += true
            println("There are no inconsistencies in the file quoting - PASS")
        }
        else {
            validationResults += false
            println("There are inconsistencies in quoting for the column - " + columnWithIssues.mkString(",") + " - FAIL")
        }
    }

    /** The function validateDateFormats validates the format of the date fields in the input file.
     *
     * @param sourceFile a data frame with the contents of the input file.
     * @param schema     Json schema of the input used
     * */
    def validateDateFormats(sourceFile: DataFrame, schema: DataFrame): Unit = {
        println("============== 5. Check if the date fields have the correct format ==============")
        val dateColumnNames = schema.where(col("attrib-datatype") === "date").select("attrib-code").map(r => r.getString(0)).collect.toList
        var dateOutput = spark.emptyDataFrame

        var status = true
        if (dateColumnNames.isEmpty != true) {
            val dateColumns = sourceFile.select(dateColumnNames.map(col): _*)
            val schemaDataTypesFormat = schema.where(col("attrib-datatype") === "date")
            val schemaDateFormat = schemaDataTypesFormat.rdd.map(x => (x.get(3), x.get(7))).collectAsMap()
            for (columns <- dateColumnNames) {
                val dateFormat = schemaDateFormat(columns.toString).toString
                dateOutput = dateColumns.withColumn(columns.toString, RemoveQuotes(col(columns.toString)))
                val invalidFormat1 = udf((columnValues: String) => invalidFormat(columnValues, dateFormat))
                val defectiveValues = dateOutput.select(col(columns.toString)).where(invalidFormat1(col(columns.toString)))
                val validateDateFormat = defectiveValues.count()
                if (validateDateFormat > 0) {
                    val sampleRecord = defectiveValues.select(col(columns.toString)).first().getString(0)
                    println("The columns " + columns.toString + " has date values which is not of the format " + dateFormat + ", sample defective value is - " + sampleRecord + " - FAIL")
                    status = false
                }
            }
            if (status == true) {
                validationResults += true
                println("There are no issues with the date format of the date fields - PASS")
            }
            else {
                validationResults += false
            }
        }
        else {
            validationResults += true
            println("There are no date fields present in the input file - PASS")
        }
    }

    /** Removes the quoting in the data frame **/
    def RemoveQuotes = udf((rawData: String) => {
        var data = rawData
        if (data == null || data.trim.isEmpty) {
            data
        } else {
            val pattern = """"[^"]*(?:""[^"]*)*"""".r
            data = pattern replaceAllIn(data, m => m.group(0).replaceAll("[\"]", ""))
            data
        }
    })

    /** Verifies if the date is of a particular format.
     *
     * @param columnValues individual values from a column
     * @param dateFormat   format of the date field from schema
     * */
    def invalidFormat(columnValues: String, dateFormat: String) = {
        if (columnValues == null || columnValues.trim.isEmpty) {
            false
        }
        else {
            Try(org.joda.time.format.DateTimeFormat forPattern dateFormat parseDateTime columnValues) match {
                case Failure(_) => true
                case _ => false
            }
        }
    }

    /** The function validateMandatoryFields verifies if the column marked as mandatory field is populated with null values.
     *
     * @param sourceFile     a data frame with the contents of the input file.
     * @param schema         Json schema of the input used
     * @param inputFileCount Total number of records in input file.
     * */
    def validateMandatoryFields(sourceFile: DataFrame, schema: DataFrame, inputFileCount: Long): Unit = {
        println("============== 6. Check mandatory fields ==============")
        val schemaMandatoryColumns = schema.select("attrib-code").where(schema("attrib-mandatory") === true).map(r => r.getString(0)).collect.toList
        val mandatoryColumns = sourceFile.select(schemaMandatoryColumns.map(col): _*)
        var mandatoryFieldsTest: Boolean = true
        var mandatoryColumnsWithIssues = new ListBuffer[String]()

        for (i <- 0 to schemaMandatoryColumns.size - 1) {
            val validateMandatoryColumns = mandatoryColumns.where(mandatoryColumns.col(schemaMandatoryColumns(i)).isNotNull).count()
            if (validateMandatoryColumns != inputFileCount) {
                mandatoryColumnsWithIssues += schemaMandatoryColumns(i)
                mandatoryFieldsTest = false
            }
        }
        if (mandatoryFieldsTest == true) {
            validationResults += true
            println("All mandatory fields are present in the file - PASS")
        }
        else {
            validationResults += false
            println("Mandatory values missing for the columns - " + mandatoryColumnsWithIssues.mkString(",") + " - FAIL")
        }
    }

    /** The function validateDataTypes verifies if the data type of the records are in line with the Power Designer schema.
     *
     * @param sourceFile     a data frame with the contents of the input file.
     * @param schema         Json schema of the input used.
     * @param inputFileCount Total number of records in input file.
     * */
    def validateDataTypes(sourceFile: DataFrame, schema: DataFrame, inputFileCount: Long): Unit = {

        var checkNullsInColumns: java.lang.Long = null

        println("============== 7. Check field data types ==============")
        val schemaDataTypes = schema.rdd.map(x => (x.get(3), x.get(5))).collectAsMap()
        var dataTypesIssues = new ListBuffer[String]()
        var dataTypeTest: Boolean = true
        var misplacedIntegerDataTypes: Boolean = false
        var misplacedDoubleDataTypes: Boolean = false
        var misplacedDataTypeIntegerColumns = new ListBuffer[String]()
        var misplacedDataTypeDoubleColumns = new ListBuffer[String]()
        val schemaDataTypesFormat = schema.where(col("attrib-datatype") === "date")
        val schemaDateFormat = schemaDataTypesFormat.rdd.map(x => (x.get(3), x.get(7))).collectAsMap()

        for ((k, v) <- schemaDataTypes) {
            checkNullsInColumns = sourceFile.where(col(k.toString).isNull || col(k.toString) == "").count()
            val fileCount = sourceFile.select(col(k.toString)).where(col(k.toString).isNotNull).count()
            val integerValues = sourceFile.select(col(k.toString), col(k.toString).cast(IntegerType).isNotNull.alias("INTEGER_CHECK")).where(col(k.toString).isNotNull)
            val filterOnlyIntegers = integerValues.where(col("INTEGER_CHECK") === true).count()

            if (v.toString.toLowerCase == "variable character") {
                if (checkNullsInColumns != inputFileCount && filterOnlyIntegers == fileCount) {
                    misplacedIntegerDataTypes = true
                    misplacedDataTypeIntegerColumns += k.toString
                    if (!(sourceFile.schema(k.toString).dataType == IntegerType || sourceFile.schema(k.toString).dataType == LongType)) {
                        println("field " + k.toString + " is not of type variable character")
                        dataTypeTest = false
                        dataTypesIssues += k.toString
                    }
                }
                else {
                    if (sourceFile.schema(k.toString).dataType != StringType) {
                        println("field " + k.toString + " is not of type variable character")
                        dataTypeTest = false
                        dataTypesIssues += k.toString
                    }
                }
            }
            if (v.toString.toLowerCase == "character") {
                if (checkNullsInColumns != inputFileCount && filterOnlyIntegers == fileCount) {
                    misplacedIntegerDataTypes = true
                    misplacedDataTypeIntegerColumns += k.toString
                    if (!(sourceFile.schema(k.toString).dataType == IntegerType || sourceFile.schema(k.toString).dataType == LongType)) {
                        println("field " + k.toString + " is not of type character")
                        dataTypeTest = false
                        dataTypesIssues += k.toString
                    }
                } else {
                    if (sourceFile.schema(k.toString).dataType != StringType) {
                        println("field " + k.toString + " is not of type character")
                        dataTypeTest = false
                        dataTypesIssues += k.toString
                    }
                }
            }
            else if (v.toString.toLowerCase == "number") {

                if (sourceFile.schema(k.toString).dataType == DoubleType) {
                    misplacedDoubleDataTypes = true
                    misplacedDataTypeDoubleColumns += k.toString
                }
                else {
                    if (checkNullsInColumns == 0 && sourceFile.schema(k.toString).dataType != IntegerType) {
                        println("field " + k.toString + " is not of type number")
                        dataTypeTest = false
                        dataTypesIssues += k.toString
                    }
                }
            }
            else if (v.toString.toLowerCase == "date") {
                val dateFormat = schemaDateFormat(k.toString).toString
                if (sourceFile.schema(k.toString).dataType != TimestampType) {
                    val invalidFormat1 = udf((columnValues: String) => invalidFormat(columnValues, dateFormat))
                    val validateDateFormat = sourceFile.where(invalidFormat1(col(k.toString))).count()
                    if (validateDateFormat > 0) {
                        println("The field " + k.toString + " is not of correct data type (DATE) or format.")
                        dataTypeTest = false
                    }
                }
            }
            else if (v.toString.toLowerCase == "float") {
                if (checkNullsInColumns == 0 && sourceFile.schema(k.toString).dataType != FloatType) {
                    println("field " + k.toString + " is not of type float")
                    dataTypeTest = false
                    dataTypesIssues += k.toString
                }
            }
        }
        if (dataTypeTest == true && misplacedIntegerDataTypes == false) {
            validationResults += true
            println("Datatype checks - PASS")
        }
        else if (dataTypeTest == true && misplacedIntegerDataTypes == true && misplacedDoubleDataTypes == true) {
            validationResults += true
            println("Datatype checks - PASS")
            println("WARNING: There are few columns for which all the values are Integers or Doubles but they are marked with different data type in schema. Please check this with the Data source.")
            println("The columns : " + misplacedDataTypeIntegerColumns.mkString(",") + " are populated with integer values but in schema they are defined as VAR CHAR or CHARACTER")
            println("The columns : " + misplacedDataTypeDoubleColumns.mkString(",") + " are populated with double values but in schema they are defined as NUMBER")
        }
        else if (dataTypeTest == true && misplacedIntegerDataTypes == true) {
            validationResults += true
            println("Datatype checks - PASS")
            println("WARNING: There are few columns for which all the values are Integers but they are marked as VAR CHAR or CHARACTER in the schema. Please check this with the Data source.")
            println("The columns are : " + misplacedDataTypeIntegerColumns.mkString(","))
        }

        else if (dataTypeTest == true && misplacedDoubleDataTypes == true) {
            validationResults += true
            println("Datatype checks - PASS")
            println("WARNING: There are few columns for which all the values are Double but they are marked as NUMBER in the schema. Please check this with the Data source.")
            println("The columns are : " + misplacedDataTypeIntegerColumns.mkString(","))
        }
        else {
            validationResults += false
            println("Datatype checks - FAIL")
        }
    }

    /** The function validateAdditionalColumns verifies if the input file has more number of columns that the input file.
     *
     * @param sourceFile a data frame with the contents of the input file.
     * @param schema     Json schema of the input used
     * */
    def validateAdditionalColumns(sourceFile: DataFrame, schema: DataFrame): Unit = {
        println("============== 8. Check missing/additional fields from input file ==============")
        val schemaColumnCount = schema.select(col("attrib-code")).count()
        val inputFileColumnCount = sourceFile.columns.toList
        if (schemaColumnCount == inputFileColumnCount.size) {
            validationResults += true
            println("There are no Missing fields in the input file - PASS")
        }
        else {
            validationResults += false
            println("There are discrepancies in fields between the input file and schema - FAIL")
        }
    }

    /** The function validateNullValues verifies if there are NULL values present in the input file.
     *
     * @param sourceFile a data frame with the contents of the input file.
     * */
    def validateNullValues(sourceFile: DataFrame): Unit = {

        val inputFileColumnCount = sourceFile.columns.toList
        println("============== 9. Check for null values in input file ==============")
        var nullValuesTest: Boolean = true
        for (i <- 0 to inputFileColumnCount.size.toInt - 1) {
            val validateNullValues = sourceFile.where(sourceFile.col(inputFileColumnCount(i).toUpperCase) === "NULL").count()
            if (validateNullValues > 0) {
                println("Field " + sourceFile.col(inputFileColumnCount(i)) + " contains illegal NULL signifier - FAIL")
                nullValuesTest = false
            }
        }
        if (nullValuesTest == true) {
            validationResults += true
            println("There are no illegal NULL values present in the file - PASS")
        }
        else {
            validationResults += false
        }
    }

    /** The function validatePrimaryKeyColumns verifies if the primary key columns or key combinations have null or duplicate values.
     *
     * @param sourceFile a data frame with the contents of the input file.
     * @param schema     Json schema of the input used
     * */
    def validatePrimaryKeyColumns(sourceFile: DataFrame, schema: DataFrame): Unit = {
        try {
            println("============== 10. Check Primary Keys==============")
            println("============== 10.1 Check for duplicate values in Primary key / key combination ==============")
            val schemaPrimarykeyColumns = schema.select("attrib-code").where(schema("attrib-primary") === true).map(r => r.getString(0)).collect.toList
            val primmaryKeyColumns = sourceFile.select(schemaPrimarykeyColumns.map(col): _*)
            val concatePrimarykeyColumns = primmaryKeyColumns.withColumn("PRIMARY_KEY", concat_ws("", primmaryKeyColumns.schema.fieldNames.map(c => col(c)): _*))
            val testDuplicates = concatePrimarykeyColumns.withColumn("DUPLICATES", count("*").over(Window.partitionBy("PRIMARY_KEY")))
            val duplicateTestOutput = testDuplicates.select("*").distinct().where(col("DUPLICATES") > 1).count()
            if (duplicateTestOutput > 0) {
                validationResults += false
                println("There are duplicates generated in the Primary key / key combination - " + schemaPrimarykeyColumns.mkString(",") + " - FAIL")
            }
            else {
                validationResults += true
                println("There are no duplicates generated for the primark key / key combination - " + schemaPrimarykeyColumns.mkString(",") + " - PASS")
            }
            println("============= 10.2. Check for null values in Primary key / key combination ==============")
            val nullPrimaryKeyValues = sourceFile.filter(schemaPrimarykeyColumns.map(name => (col(name).isNull)).reduce(_ and _)).count()
            if (nullPrimaryKeyValues > 0) {
                validationResults += false
                println("There are null values generated for the primary key combination - " + schemaPrimarykeyColumns.mkString(",") + " - FAIL")
            }
            else {
                validationResults += true
                println("There are no null values generated for the Primary key / key combination - " + schemaPrimarykeyColumns.mkString(",") + " - PASS")
            }
        }
        catch {
            case analysisException: AnalysisException => {
                println("Verify if the input file has correct encoding (UTF-8) and correct headers.")
            }
            case _ => {
                println("Verify if the input file and Schema is valid with correct header and correct file path")
            }
        }

    }

    /** The function checkFileExistence checks if the input file and schema is available in the folder path.
     *
     * @param inputFilePath Input file from source.
     * @param schemaPath    Json schema of input.
     * @param sc            spark context.
     * */
    def checkFileExistence(inputFilePath: String, schemaPath: String, sc: SparkContext) = {
        val conf = sc.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        if (!fs.exists(new org.apache.hadoop.fs.Path(inputFilePath))) {
            println("Input file not found in location : " + inputFilePath)
            throw new FileNotFoundException
        }
        if (!fs.exists(new org.apache.hadoop.fs.Path(schemaPath))) {
            println("Schema file not found in location : " + schemaPath)
            throw new FileNotFoundException
        }
    }
}
