============================
lcs-appsync-base-datasource
============================

Grabs the AWS RDS secrets for a user based on the claims in
their JWT (passed from resolver) and executes a query passed
by the resolver. Also checks to make sure the user has the appropriate
role and nodules to execute the passed query if required.

AWS Permissions Required
------------------------
.. code-block:: JSON

  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Sid": "SecretsManagerDbCredentialsAccess",
              "Effect": "Allow",
              "Action": [
                  "secretsmanager:GetSecretValue",
                  "secretsmanager:PutResourcePolicy",
                  "secretsmanager:PutSecretValue",
                  "secretsmanager:DeleteSecret",
                  "secretsmanager:DescribeSecret",
                  "secretsmanager:TagResource"
              ],
              "Resource": "arn:aws:secretsmanager:*:*:secret:rds-db-credentials/*"
          },
          {
              "Sid": "RDSDataServiceAccess",
              "Effect": "Allow",
              "Action": [
                  "secretsmanager:CreateSecret",
                  "secretsmanager:ListSecrets",
                  "secretsmanager:GetRandomPassword",
                  "tag:GetResources",
                  "rds-data:BatchExecuteStatement",
                  "rds-data:BeginTransaction",
                  "rds-data:CommitTransaction",
                  "rds-data:ExecuteStatement",
                  "rds-data:RollbackTransaction"
              ],
              "Resource": "*"
          }
      ]
  }



Environment Variables
---------------------
**DB_ARN**
  ARN for the database cluster
**DB_NAME**
  Name of the database to use
**ACCOUNT**
  AWS account number
**REGION**
  AWS region the DB is in
**HL7_ETL_ROLE_ARN**
  Arn for the role that HL7 ETL will use
**NLP_ROLE_ARN**
  Arn for the role that NLP will use
**DICOM_ETL_ROLE_ARN**
  Arn for the role that DICOM ETL will use

Handler
-------
``function.handler``

Example AWS Appsync resolver
----------------------------

.. code-block:: python

  {
      "version": "2018-05-29",
      "operation": "Invoke",
      "payload": {
        "claims": $ctx.identity.claims, ##### Always should be here,
        "requiredNodules": ["module_1", "module_3"]  ## If provided then nodules in the user's claim must contain all items from list
        "allowedRoles": ["user", "admin"],  ## If provided then the role in the user's claim must be in this list
        "query":  "INSERT INTO patients VALUES(DEFAULT, :FIRSTNAME, :LASTNAME)", ## Executed
        "parameters": [    ## List of parameters that are passed to boto3's execute_statement() method
          {'name': 'LASTNAME', 'value': {'stringValue': $ctx.args.lastname'}},
          {'name': 'FIRSTNAME', 'value': {'stringValue': $ctx.args.firstname'}}
        ]
      }
  }
