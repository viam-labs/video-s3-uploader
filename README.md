This module calls save on a video store and uploads it to an S3 bucket.

This is a generic service that runs on an interval specified in the module attributes.

It requires a running [video-store module](https://github.com/viam-modules/video-store/tree/main).

## Config attributes

| name | type | explanation |
| ---- | ---- | ----------- |
| video_store | string | name of video-store component |
| interval | int | interval in minutes to call save and upload file |
| local_path | string | absolute path to where the video-store component will save videos. |
| aws_region | string | aws region the bucket is in| 
| bucket_name | string | name of existing aws bucket name |
| aws_key_id | string | aws secret key id |
| aws_key_value | string | aws secret key value |

### Example config
```json
{
  "video_store": "",
  "interval": 1,
  "local_path": "",
  "aws_region": "us-east-2",
  "bucket_name": "bucket",
  "aws_key_id": "keyID",
  "aws_key_value": "keyValue"
}
```