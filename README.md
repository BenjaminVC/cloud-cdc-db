# cloud-cdc-db
A python script that uploads valid xml or json objects to AWS S3 bucket with a tag, triggering a lambda that parses and stores the data to AWS RDS.
Includes Dockerfile for Lambda function to build, tag, and push image to AWS ECR.
