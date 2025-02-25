#!/usr/bin/env node
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as cdk from 'aws-cdk-lib';
import { DynamoTableReplicatorStack } from '../lib/dynamo-table-replicator-stack';
import * as config from '../config.json';

const app = new cdk.App();

new DynamoTableReplicatorStack(app, 'DynamoTableReplicatorStack',{

    tableName: config.tableName, 
    tableStreamArn: config.tableStreamArn,
    glueDatabaseName: config.glueDatabaseName,
    glueTableName: config.glueTableName,
    glueTableSchema: {
        success: 'string',
        campaign_activity_id: 'string',
        awsaccountid: 'string',
        destination_phone_number: 'string',
        iso_country_code: 'string',
        record_status: 'string',
        eventsource: 'string',
        message_type: 'string',
        origination_phone_number: 'string',
        sender_request_id: 'string',
        buffered: 'string',
        price_in_millicents_usd: 'string',
        journey_id: 'string',
        campaign_id: 'string',
        message_id: 'string',
        eventname: 'string'
      },
    env: {
        account: process.env.CDK_DEFAULT_ACCOUNT!,
        region: process.env.CDK_DEFAULT_REGION!,
      }

});
