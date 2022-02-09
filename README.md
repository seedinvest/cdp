# cdp
Customer Data Platform

# Manual steps to deploy glue
```
cd glue-etl
mvn clean install
# Manually copy target/cdp-x.x-shaded.jar => s3://aws-glue-jars-301027959319-us-east-1
# Update glue job script (if changed)
```

# Manual steps to deply Lambda
```
```
