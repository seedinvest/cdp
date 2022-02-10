#!/bin/bash
rm -f founders.zip
zip -x release-dev.sh -r /tmp/founders.zip .
aws lambda update-function-code --function-name cdp-founders-segment-dev --zip-file fileb:///tmp/founders.zip

