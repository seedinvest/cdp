import * as cdk from "@aws-cdk/core";
import * as sns from "@aws-cdk/aws-sns";
import * as sqs from "@aws-cdk/aws-sqs";
import * as subs from '@aws-cdk/aws-sns-subscriptions';
import {Code, Function, Runtime} from "@aws-cdk/aws-lambda";
import * as path from "path";
import {SqsEventSource} from "@aws-cdk/aws-lambda-event-sources";
import { Duration } from "@aws-cdk/core";

export interface CrmToSegmentStackProps extends cdk.StackProps {
  environments: String[],
};

export class CrmToSegmentStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: CrmToSegmentStackProps) {
    super(scope, id, props);

    for (const env of props.environments) {

      const dlQueue = new sqs.Queue(this, `${env}-CRMUserTrackingDeadLetterQueue`, {
        queueName: `${env}-CRMUserTrackingDeadLetterQueue`,
        retentionPeriod: Duration.days(14),
      });

      const queue = new sqs.Queue(this, `${env}-CRMUserTrackingQueue`, {
        queueName: `${env}-CRMUserTrackingQueue`,
        retentionPeriod: Duration.days(2),
        deadLetterQueue: {
          queue: dlQueue,
          maxReceiveCount: 5,
        }
      });

      // TODO: Add production user iam to be able to publish to this topic
      const topic = new sns.Topic(this, `${env}-CRMUserTrackingTopic`, {
        topicName: `${env}-CRMUserTrackingTopic`
      });
      topic.addSubscription(new subs.SqsSubscription(queue));

      new Function(this, `${env}-LambdaFunction`, {
        functionName: `${env}-crm-to-segment-lambda`,
        runtime: Runtime.NODEJS_14_X,
        handler: "index.handler",
        code: Code.fromAsset(path.join(__dirname, "/../assets/segment/")),
        timeout: cdk.Duration.seconds(30),
        events: [
          new SqsEventSource(queue),
        ],
        environment: {  // NOTE:!! This will overwrite and set back to these default values
          'write_key': 'place_holder',  // Segment Write API key
          'is_dry_run': 'true',         // If true we will consume the queue message and skip sending to segment (Local/Debugging)
          'is_debug': 'false'           // More logging
        },
      });

      new cdk.CfnOutput(this, `${env}-snsTopicArn`, {
        value: topic.topicArn,
        description: 'The arn of the SNS topic',
      });
    }
  }
}
