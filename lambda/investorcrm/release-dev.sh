#!/bin/bash
rm -f /tmp/package.zip
zip -x release-dev.sh -r /tmp/package.zip .

aws lambda get-function --function-name cdp-investor-action-segment-dev 1>/dev/null 
if [[ $? -eq 0 ]] 
then
    aws lambda update-function-code --function-name cdp-investor-action-segment-dev --zip-file fileb:///tmp/package.zip
else 
    aws lambda create-function --function-name cdp-investor-action-segment-dev --zip-file fileb:///tmp/package.zip --role arn:aws:iam::301027959319:role/segment-dev-lambda-s3-role --runtime nodejs14.x --handler index.handler
fi