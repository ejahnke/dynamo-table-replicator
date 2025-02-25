// lib/stack-props.ts
import * as cdk from 'aws-cdk-lib';

export interface DynamoTableReplicatorStackProps extends cdk.StackProps {
  tableName: string;
  tableStreamArn: string;
  glueDatabaseName: string;
  glueTableName: string;
  glueTableSchema: { [key: string]: any };
  env: { account: string, region: string }
}
