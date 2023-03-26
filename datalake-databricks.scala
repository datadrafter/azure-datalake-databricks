val appID = ""
val password = ""
val tenantID = ""
val containerName = ""
var storageAccountName = ""

val configs =  Map("fs.azure.account.auth.type" -> "OAuth",
       "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id" -> appID,
       "fs.azure.account.oauth2.client.secret" -> password,
       "fs.azure.account.oauth2.client.endpoint" -> ("https://login.microsoftonline.com/" + tenantID + "/oauth2/token"),
       "fs.azure.createRemoteFileSystemDuringInitialization"-> "true")

dbutils.fs.mount(
source = "abfss://" + containerName + "@" + storageAccountName + ".dfs.core.windows.net/",
mountPoint = "/mnt/data",
extraConfigs = configs)



val df = spark.read.csv("/mnt/data/" + containerName + "/samples.csv")
display(df)



val df = spark.read.option("header", "true").csv("/mnt/data/" + containerName + "/samples.csv")
display(df)



val selected = df.select("sampleId", "title")
selected.save.csv("/mnt/data/" + containerName + "/output/samples-mount.csv")