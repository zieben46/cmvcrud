from pyspark.sql import SparkSession

# Path to your .jar file (adjust based on your project structure)
jar_path = "/path/to/dbadminkit/lib/DatabricksJDBC42.jar"

spark = SparkSession.builder \
    .appName("dbadminkit_jdbc") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

jdbc_url = "jdbc:databricks://<host>:443;httpPath=/sql/protocolv1/o/<org-id>/<cluster-id>;AuthMech=3;UID=token;PWD=<your-token>"
df = spark.read.jdbc(url=jdbc_url, table="my_table")
print(df.collect())
spark.stop()





import jpype
import jpype.imports

# Path to your .jar file
jar_path = "/path/to/dbadminkit/lib/DatabricksJDBC42.jar"

# Start JVM and add .jar to classpath
jpype.startJVM(classpath=[jar_path])

# Import Java classes
from java.sql import DriverManager

# JDBC connection
jdbc_url = "jdbc:databricks://<host>:443;httpPath=/sql/protocolv1/o/<org-id>/<cluster-id>;AuthMech=3;UID=token;PWD=<your-token>"
conn = DriverManager.getConnection(jdbc_url)
stmt = conn.createStatement()
rs = stmt.executeQuery("SELECT * FROM my_table")
while rs.next():
    print(rs.getString("col1"))
conn.close()
jpype.shutdownJVM()