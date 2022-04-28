#!/bin/bash

# TODO: update with github action or something else
rm -f /tmp/localCrmToSegment.zip
zip -x release-dev.sh -r /tmp/localCrmToSegment.zip .
aws lambda update-function-code --function-name Local-crm-to-segment-lambda --zip-file fileb:///tmp/localCrmToSegment.zip
