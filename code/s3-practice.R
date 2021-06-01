library(aws.s3)



endpoint_url="idbllzgo8pgz.compat.objectstorage.us-ashburn-1.oraclecloud.com"


Sys.setenv(
  "AWS_ACCESS_KEY_ID" = Sys.getenv("S3_KEY_ID"),
  "AWS_SECRET_ACCESS_KEY" = Sys.getenv("S3_ACCESS_KEY"),
  "AWS_DEFAULT_REGION" = "us-ashburn-1",
  "AWS_S3_ENDPOINT" = endpoint_url
)

bucketlist(region = "")


# specify keys in-line
get_bucket(
  bucket = "virginia-court-data-code4cville",
  key = Sys.getenv("S3_KEY_ID"),
  secret = Sys.getenv("S3_ACCESS_KEY"), region = ""
)