
# Serverless ETL Framework

## Instructions

1. Create Target Stacks first followed by the Source Stack as some resources in the Source Template depend on the Target resources
2. Members of SNS Subscription list must confirm the subscription by clicking on the link sent via email as soon as the stack is created
3. Connect to RDS MySQL DB Instance using a MySQL Client application (such as MySQL Workbench)
    1. Navigate to Stack Outputs and find the HTTPS URL to clone the CodeCommit Repo against the Key **CodeCommitRepoCloneUrl**
    2. Navigate to Stack Outputs and find the Secret ARN for RDS Creds against the Key **RDSDBInstanceCredentials**
    3. Navigate to AWS Secrets Manager to retrieve RDS creds using the above Secret ARN
    4. Connect to RDS MySQL DB Instance using the Host, Port, Username & Password retrieved from above steps
4. Insert entries into configuration tables
5. Create Step Functions Workflow by navigating to Stack Outputs and clicking on the URL against the Key **CreateWorkflowFunctionUrl**

## Constraints

- Ensure that there are at least two Availability Zones in the Region where Stack is being created.
- Ensure that the region selected for Target Environment(s) is the same as that of the source Environment.
- Parameter ProjectCode for Target environment(s) should match with that for Source environment.
