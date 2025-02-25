import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as eventsources from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';
import * as path from 'path';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import { DynamoTableReplicatorStackProps } from './stack-props';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';


export class DynamoTableReplicatorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DynamoTableReplicatorStackProps) {
    super(scope, id, props);

     // Create S3 bucket for Iceberg table data
     const dataBucket = new s3.Bucket(this, 'pinpoint_table_storage', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      versioned: true, // Important for Iceberg's time travel feature
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    // Create S3 bucket for Firehose backup
    const backupBucket = new s3.Bucket(this, 'firehose_backup_bucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    // Create Glue Database
    const glueDatabase = new glue.CfnDatabase(this, 'IcebergDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: props.glueDatabaseName,
        description: `Database for ${props.tableName} Iceberg table`,
      },
    });

    // Create IAM role for Glue
    const glueRole = new iam.Role(this, 'GlueServiceRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    // Add necessary permissions
    dataBucket.grantReadWrite(glueRole);
    glueRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
    );

     // Convert the JSON schema to Glue column format
    const columns = Object.entries(props.glueTableSchema).map(([name, type]) => ({
      name,
      type: type as string,
    }));

    // Create Glue Table
    const glueTable = new glue.CfnTable(this, 'IcebergTable', {
      databaseName: props.glueDatabaseName,
      catalogId: this.account,
      tableInput: {
        name: props.glueTableName,
        tableType: 'EXTERNAL_TABLE',
        parameters: {
          //'table_type': 'ICEBERG',
          'format': 'parquet',
          'write_target_data_file_size_bytes': '536870912', // 512MB
          'write_format.compression': 'snappy',
          'format-version': '2',
        },
        storageDescriptor: {
          columns,
          location: `s3://${dataBucket.bucketName}/${props.tableName}`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            parameters: {
              'serialization.format': '1',
            },
          },
        },
      },
      openTableFormatInput: {
        icebergInput: {
          metadataOperation: 'CREATE'
        },
      }
    });

    // Add dependencies
    glueTable.addDependency(glueDatabase);

   


    // Reference the existing table
    const table = dynamodb.Table.fromTableAttributes(this, 'ExistingTable', {
      tableName: props.tableName,
      tableStreamArn: props.tableStreamArn,
    });

    // Create Lambda function to process stream events
    const streamProcessor = new lambda.Function(this, 'StreamProcessor', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      environment: {
        DELIVERY_STREAM_NAME: `${props.tableName}-stream`
      },
      code: lambda.Code.fromInline(`# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import os
import boto3

firehose = boto3.client("firehose", region_name='ca-central-1')

def safe_get_value(record, key, default=None):
    """Safely get a value from DynamoDB record"""
    try:
        if key not in record:
            return default
        
        if 'NULL' in record[key]:
            return None
        elif 'S' in record[key]:
            return record[key]['S']
        else:
            return default
    except Exception:
        return default

def handler(event, context):

    print(event)

    # Get the delivery stream name from environment variable
    delivery_stream_name = os.environ['DELIVERY_STREAM_NAME']

    for each_record in event.get('Records', []):
            try:
                # Safely get NewImage
                record = each_record.get('dynamodb', {}).get('NewImage', {})

                eventname = "insert" if each_record.get('eventName',{}) == "INSERT" else "update" if each_record.get('eventName',{}) == "MODIFY" else None
                # Create dictionary with safe value extraction
                processed_record = {
                    'success': safe_get_value(record, 'SUCCESS'),
                    'campaign_activity_id': safe_get_value(record, 'campaign_activity_id'),
                    'awsaccountid': safe_get_value(record, 'awsAccountId'),                   
                    'destination_phone_number': safe_get_value(record, 'destination_phone_number'),
                    'iso_country_code': safe_get_value(record, 'iso_country_code'),
                    'record_status': safe_get_value(record, 'record_status'),
                    'eventsource': safe_get_value(record, 'eventSource'),
                    'message_type': safe_get_value(record, 'message_type'),
                    'origination_phone_number': safe_get_value(record, 'origination_phone_number'),
                    'sender_request_id': safe_get_value(record, 'sender_request_id'),
                    'buffered': safe_get_value(record, 'BUFFERED'),
                    'price_in_millicents_usd': safe_get_value(record, 'price_in_millicents_usd'),
                    'journey_id': safe_get_value(record, 'journey_id'),
                    'campaign_id': safe_get_value(record, 'campaign_id'),                    
                    'message_id': safe_get_value(record, 'message_id'),
                    'eventname': eventname
                }

                
                print(processed_record)
                entry = {"Data": json.dumps(processed_record)}

                if eventname in ('insert', 'update'):
                    print('invoking firehose!')
                    response = firehose.put_record(
                        DeliveryStreamName=delivery_stream_name, Record=entry
                    )
                
            except Exception as e:
                print(f"Error processing record: {str(e)}")
                continue



    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
      `),
    });

    // Create Lambda firehose transformer
    const transformer = new lambda.Function(this, 'FirehoseTransformer', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import base64
import json
import os
print('Loading function')


def handler(event, context):
    
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = json.loads(base64.b64decode(record['data']).decode('utf-8'))
        print(payload)

        final_payload = payload
        # Do custom processing on the payload here
        # convert the json to iceberg

        payload = json.dumps(final_payload)
        print(payload)
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload.encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}

      `),
    });

    // Add DynamoDB stream as event source for Lambda
    const eventSource = new eventsources.DynamoEventSource(table, {
      startingPosition: lambda.StartingPosition.LATEST,
      batchSize: 100,
      retryAttempts: 3,
      bisectBatchOnError: true,
      maxBatchingWindow: cdk.Duration.seconds(30),
      filters: [
        lambda.FilterCriteria.filter({
          eventName: ['INSERT', 'MODIFY']
        })
      ]
    });

    // Add the event source to the Lambda function
    streamProcessor.addEventSource(eventSource);

    // Grant the Lambda function permissions to read from the stream
    table.grantStreamRead(streamProcessor);

    // Optional: Add CloudWatch Logs permissions
    streamProcessor.addToRolePolicy(new cdk.aws_iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents'
      ],
      resources: ['*']
    }));

    

    // Create IAM role for Firehose
    const firehoseRole = new iam.Role(this, 'FirehoseRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    // Add necessary permissions for Firehose
    firehoseRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:GetTable',
        'glue:GetTableVersion',
        'glue:GetTableVersions',
        'glue:GetDatabase',
        'glue:UpdateTable'
      ],
      resources: [
        `arn:aws:glue:${this.region}:${this.account}:catalog`,
        `arn:aws:glue:${this.region}:${this.account}:database/${props.glueDatabaseName}`,
        `arn:aws:glue:${this.region}:${this.account}:database/${props.glueDatabaseName}/*`,
        `arn:aws:glue:${this.region}:${this.account}:table/${props.glueDatabaseName}/${props.glueTableName}`,
        `arn:aws:glue:${this.region}:${this.account}:table/${props.glueDatabaseName}/${props.glueTableName}/*`
      ]
    }));

    firehoseRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:AbortMultipartUpload',
        's3:GetBucketLocation',
        's3:GetObject',
        's3:ListBucket',
        's3:ListBucketMultipartUploads',
        's3:PutObject',
        's3:DeleteObject'
      ],
      resources: [
        `arn:aws:s3:::${dataBucket.bucketName}/${props.tableName}`,
        `arn:aws:s3:::${dataBucket.bucketName}/${props.tableName}/*`
      ]
    }));

    backupBucket.grantWrite(firehoseRole);
    transformer.grantInvoke(firehoseRole);

    // Create Firehose delivery stream
    const deliveryStream = new firehose.CfnDeliveryStream(this, 'IcebergDeliveryStream', {
      deliveryStreamName: `${props.tableName}-stream`,
      deliveryStreamType: 'DirectPut',
      
      icebergDestinationConfiguration: {
        catalogConfiguration: {
          catalogArn: `arn:aws:glue:${this.region}:${this.account}:catalog`, 
        },
        roleArn: firehoseRole.roleArn,
        s3Configuration:{
          bucketArn:backupBucket.bucketArn,
          roleArn: firehoseRole.roleArn,
          errorOutputPrefix: 'errors',
          compressionFormat: 'UNCOMPRESSED',
          prefix: 'errors/',
          bufferingHints: {
              intervalInSeconds: 60,
              sizeInMBs: 128
          },
          cloudWatchLoggingOptions: {
            enabled: true,
            logGroupName: `/aws/firehose/${props.glueTableName}-stream`,
            logStreamName: 'IcebergDelivery'
          }
        },
        processingConfiguration: {
          enabled: true,
          processors: [{
            type: 'Lambda',
            parameters: [{
              parameterName: 'LambdaArn',
              parameterValue: transformer.functionArn
            }]
          }]
        },
         destinationTableConfigurationList: [{
           destinationDatabaseName:  props.glueDatabaseName ,
           destinationTableName:  props.glueTableName  
         }]
        ,

        bufferingHints: {
          intervalInSeconds: 15,
          sizeInMBs: 128
        },
        
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: `/aws/firehose/${props.glueTableName}-stream`,
          logStreamName: 'IcebergDelivery'
        },
        s3BackupMode: 'FailedDataOnly'
      }
    });

    deliveryStream.addDependency(glueTable);
  
    streamProcessor.addToRolePolicy(new cdk.aws_iam.PolicyStatement({
      actions: [
        'firehose:PutRecord'
      ],
      resources: [deliveryStream.attrArn]
    }));

    // Add CloudWatch Logs permissions
    firehoseRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents'
      ],
      resources: ['*']
    }));
  }
}
