#!/bin/bash
rm -f /tmp/basic.zip
zip -x release-dev.sh -r /tmp/basic.zip .
aws lambda update-function-code --function-name cdp-basic-segment-dev --zip-file fileb:///tmp/basic.zip
