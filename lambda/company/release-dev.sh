#!/bin/bash
rm -f company.zip
zip -x release-dev.sh -r /tmp/company.zip .
aws lambda update-function-code --function-name cdp-company-segment-dev --zip-file fileb:///tmp/company.zip

