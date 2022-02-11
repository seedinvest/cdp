# cdp
Customer Data Platform

# Deps
* A configured aws cli
* Java 8 / Maven
* Node 14


# Build and manual steps to deploy glue
```
cd glue-etl
mvn clean install
# Manually copy target/cdp-x.x-shaded.jar => s3://aws-glue-jars-301027959319-us-east-1
# Manually upload scala job file to s3://aws-glue-scripts-301027959319-us-east-1
```

# Build and deploy lambda
## Development
```
# founders
cd lamba/founders
./release-dev.sh

# company
cd lamba/company
./release-dev.sh
```

# Permissions
## Lambda
* Read s3://aws-glue-segment-dev-301027959319-us-east-1/** (SegmentLambdaExecute-DEV)

## Glue
* Write s3://aws-glue-segment-dev-301027959319-us-east-1


# Paths and locations
## Scala OUTPUT and Lambda INPUT development s3 buckets
* s3://aws-glue-segment-dev-301027959319-us-east-1

## Glue Jars (dependencies for the Scala jobs are included in this jar(s))
* s3://aws-glue-jars-301027959319-us-east-1

## Glue Scripts (scala jobs must link to a script on s3. I don't understand this yet)
* s3://aws-glue-scripts-301027959319-us-east-1

## Glue Scripts (scala jobs must link to a script on s3. I don't understand this yet)
* s3://aws-glue-scripts-301027959319-us-east-1
