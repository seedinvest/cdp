# CDP
Customer Data Platform

# External Tools
* segment
* hubspot
* aws glue
* aws lambda

# Build Dependencies
* A configured aws cli
* Java 8 / Maven
* Node 14


# Build and manual steps to deploy glue
```
cd glue-etl
mvn clean install
# Manually copy target/cdp-x.x-shaded.jar => s3://aws-glue-jars-301027959319-us-east-1
aws s3 cp ./target/cdp-1.0-SNAPSHOT-shaded.jar s3://aws-glue-jars-301027959319-us-east-1/ 
# Manually upload scala source code file to s3://aws-glue-scripts-301027959319-us-east-1
aws s3 cp ./src/main/scala/com/circle/data/jobs/InvestorActionProcessor.scala s3://aws-glue-assets-301027959319-us-east-1/scripts/
```

# Build and deploy lambda
## Development
```
# deploy the lambda to import investor basic information
cd lamba/basic
npm install
./release-dev.sh
```

# Permissions
## Lambda
* Read s3://aws-glue-segment-dev-301027959319-us-east-1/** (SegmentLambdaExecute-DEV)

## Glue
* Write s3://aws-glue-segment-dev-301027959319-us-east-1

# Development Paths and locations
## Scala OUTPUT and Lambda INPUT s3 buckets
* s3://aws-glue-segment-dev-301027959319-us-east-1

## Glue Jars (dependencies for the Scala jobs are included in this jar(s))
* s3://aws-glue-jars-301027959319-us-east-1

## Glue Scripts (scala jobs must link to a script on s3. I don't understand this yet)
* s3://aws-glue-scripts-301027959319-us-east-1

# Schedules
* Founders: Glue job runs 10pm nightly
* Companies: Glue job runs at 11pm nightly

# Cleanup / audit / todo
* cleanup IAM
* dev vs prod setup
* ci/cd
* audit/data integ
