#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CrmToSegmentStack } from '../lib/crm-to-segment';

const app = new cdk.App();
new CrmToSegmentStack(app, 'CrmToSegmentStack', {
  // environments: ['Local'],
  environments: ['Local', 'Dev', 'Prod'],
});
