# ms.source.s3.parameters

**Category**: [execution](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/execution-parameters.md)

**Type**: string

**Format**: A JsonObject 

**Default value**: empty JsonObject (considered as blank)

**Related**:


## Description

`ms.source.s3.parameters` specifies parameters for S3 connection.
It can have the following attributes:

- **region**: string, one of [aws region codes](https://docs.aws.amazon.com/general/latest/gr/rande.html)
- **read_timeout_seconds**: integer, read time out in seconds
- **write_timeout_seconds**: integer, write time out in seconds
- **connection_timeout_seconds**: Sets the socket to timeout after failing to establish a connection with the server after milliseconds.
- **connection_max_idle_millis**:  Sets the socket to timeout after timeout milliseconds of inactivity on the socket.
   
### Example

`ms.source.s3.parameters={"region" : "us-east-1"}`

[back to summary](https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/summary.md#mssources3parameters)      