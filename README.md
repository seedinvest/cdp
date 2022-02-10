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

# Glue Destination
## Development
```
# company s3 destination
s3://aws-glue-segment-dev-301027959319-us-east-1/cdp/segment/group/

# deploy
cd lambda/company
rm company.zip
zip -r company.zip .
aws lambda update-function-code --function-name cdp-company-segment-dev --zip-file fileb://company.zip
```


# Permissions
## Lambda
* Read s3://aws-glue-segment-dev-301027959319-us-east-1/** (SegmentLambdaExecute-DEV)

## Glue
* Write s3://aws-glue-segment-dev-301027959319-us-east-1
