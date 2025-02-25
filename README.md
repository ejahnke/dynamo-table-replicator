# DynamoDB Table Replicator

This AWS CDK application creates a data pipeline that replicates DynamoDB table data to Apache Iceberg format in S3, enabling efficient analytics and querying capabilities.

## Architecture

The application sets up the following components:

- DynamoDB Stream processing via Lambda
- Kinesis Firehose for data delivery
- Apache Iceberg table in AWS Glue Data Catalog
- S3 buckets for data storage and backup
- IAM roles and permissions

DynamoDB -> DynamoDB Stream -> Lambda -> Kinesis Firehose -> S3 (Iceberg Format)

## Prerequisites
- AWS CDK CLI installed
- Node.js and TypeScript
- AWS account and credentials configured
- Python 3.11 (for Lambda functions)
- Source DynamoDB table with Stream Events enabled

## Infrastructure Components
### Storage
- Data Bucket : S3 bucket for storing Iceberg table data (versioned)
- Backup Bucket : S3 bucket for Kinesis Firehose backup storage

### Data Processing
- Stream Processor Lambda : Processes DynamoDB Stream events and forwards to Kinesis Firehose
- Transformer Lambda : Transforms records before writing to Iceberg format
- Kinesis Firehose : Manages data delivery to S3 in Iceberg format

### Data Catalog
- Glue Database : Houses the Iceberg table metadata
- Glue Table : Defines the schema and storage format for the data

### Configuration
The application can be configured through the following properties:

- tableName: Name of the source DynamoDB table
- tableStreamArn: ARN of the DynamoDB Stream
- glueDatabaseName: Name for the Glue Database
- glueTableName: Name for the Glue Table
- glueTableSchema: Schema definition for the Glue Table

## Deployment
- Install dependencies:
```
npm install
```
- Configure your application settings in config.json
- Deploy the stack
```
cdk deploy
```

## Data Flow
- Changes in the DynamoDB table trigger stream events
- Lambda function processes these events and sends them to Kinesis Firehose
- Firehose transforms the data using a Lambda function
- Data is written to S3 in Apache Iceberg format
- Glue Data Catalog maintains the table metadata

## Features
- Real-time data replication from DynamoDB
- Data transformation support
- Backup storage for failed records
- Apache Iceberg format support
- Automated error handling and retries
- CloudWatch logging for monitoring

## Security
The application implements the following security measures:
- S3 bucket encryption
- IAM role-based access control
- Secure Lambda execution environments
- Managed service integrations

## Monitoring
The application can be monitored through:

- CloudWatch Logs for Lambda functions
- Firehose delivery metrics
- S3 bucket metrics
- Glue Data Catalog operations

## Clean Up
To avoid incurring charges, delete the stack when no longer needed:
```
cdk destroy
```
Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

