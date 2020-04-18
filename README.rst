A framework tying Aurora Serverless Postgres together with AWS Cognito and Appsync in a multi-tenant application
=================================================================================================================
The package includes utilities for creating a database schema with the needed tables for users and tenants, creating Necessary Cognito webooks, and resolving Appsync queries.

========================
# Basic Security Concepts

Authentication in Aurora Serveless is done via AWS secrets that contain database credentials. pg-cognition creates users in the database that map to credentials in AWS Secrets Manager to provide authorization
on the database layer instead of requiring it in the application layer. This results in a high level of security due to the fact that an exploit in the application layer will not allow a user to access resources
that they are not granted in the database layer.

## Database roles
The basis of authorization is achieved through a scaffolding of database roles and Row Level Security Policies. The following roles dictate what users can do in the users and tenants table in the pg-cognition schema,
which controls authorization.

  + **application_admins** - Have the keys to the kingdom. They can modify any attribute of any user or tenant
  + **tenant_admins** - Can modify any attribute of a user (except their database username) that belongs to their tenant and change their tenant’s display name
  + **tenant_users** - Can modify their own basic info

## Tenants and schemas
pg-cognition creates a multi-tenant system by segregating tenants into their own schemas. This creates a layer of isolation in data without having to create RLS policies for any piece of data that you want secregated.
You can of course create a single tenant and assign all users to that tenant if you wish for your application to behave differently. Users are “locked into” their tenants’s schema through role inheritance.

Before creating a tenant you first create a template schema named “tenant_template”. This schema is used as the template for any user you create and contain any database objects that you want, including

  + tables
  + triggers
  + functions
  + RLS Policies
  + etc

When a tenant is created this schema is cloned, along with all of its child objects. There also an admin and a tenant role that are created with only permissions on that schema.
These roles inherit the tenant_admins and tenant_users roles (respectively). Fine tuning permissions can be done in three different ways:

+ Modifying Grants for the tenant level roles to apply new permissions only to the users and/or admins in that tenant for only their schema
+ Modifying Grants to the tenant_admins/tenant_users roles, which will grant these permissions to the users/admins of all tenants
+ Applying RLS to a specific schema

## Appsync
This package includes utilities for creating a Lambda function that can function as an Appsync datasource. This works by grabbing the Cognito identity or IAM arn of the entitiy
who made the call to appsync and then performing the query using their db credentials. Using this datasource makes creating resolvers simple. By default we handle single or batch
invocations, returning the data in the format that Appsync expects from our method. Resolvers simply define a query and a list of parameters that will be passed to the call to Aurora.
No authorization has to happen in the resolver itself and no parsing has to happen in the response template to organize the data into the format that Appsync expects.

Features:

  + Batch or single invocations work without any extra configuration
  + Database credentials are fetched based on the Cognito user who made the Appsync call
  + Additional overrides allow IAM roles and IAM Assumed roles to be mapped to specific database credentials
  + Results are automatically formatted into a list of dictionaries with the column names as keys (or a list of lists in the case of a batch invoke) using aurora-prettyparser package

## Cognito
Integration with Cognito is what glues the application layer to the database authorization layer. This starts with implementing Cognito hooks that tie users in the database users table to a cognito user.
Standard user creation workflow:

  + A user is invited to the application
  + A row representing that user is created in the users table (with no database credentials and no way to access the app) and is set to an “invited” state
  + When a user attempts to create a cognito user in the application’s user pool the Cognito hooks will prevent a signup if that email address does not exist in the user’s table in the “invited” state
  + If the user successfully created in Cognito (eg: was invited and Cognito user creation is successful) then a function can be triggered that will
    - Generate a random database username and role
    - Assign that username to the new user
    - Create the secret for that database user in Secrets Manager
    - Grant the tenant user/admin role to the new user

### Hooks for login
When a user attempts to log into the application we can reject them with a PreAuthentication hook if they do not exist in the application or are not in the “active” status. This allows us to to suspend users
by simply changing their user status in the database. This also blocks attemps to guess/brute force Cognito credentials since the username would not only have to exist in Cognito, but also be active in the application.
Once a user is successfully logged in we can add claims to their JWT to be used in the application using the Cognito module in the PreTokenGeneration hook.
This separation of Cognito users and database users has several advantages.

  + Getting a Cognito login doesn’t guarantee access to the database
  + A user can be removed in the application without removing their Cognito user
  + Users can be suspended in the application without having to touch their Cognito user
  + The use of Cognito hooks prevents unauthorized logins from existing Cognito users that have been disabled in the application

[Read the full code Documentation HERE](./docs/index.html)
