# aws-lambda-unzip
Function for AWS Lambda to extract zip files uploaded to S3

Files are extracted in place in the same bucket as where the zip file was uploaded. Any files present with the same name are overwritten. The zip file is deleted at the end of the operation.

## Necessary permissions
In order to remove the uploaded zip file, the role configured in your Lambda function should have a policy looking like this:
```
{
        "Effect": "Allow",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
        ],
        "Resource": [
            "arn:aws:s3:::mybucket"
	]
}
```

## Packaging for deployment
Maven is already configured to package the .jar file correctly for deployment into Lambda. Just run
```
mvn clean package
```
The packaged file will be present in your `target/` folder.
