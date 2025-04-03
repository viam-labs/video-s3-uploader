# Video Store Uploader Module

This module periodically calls the `save` command on a [video-store module](https://github.com/viam-modules/video-store/tree/main) and uploads the resulting video files to an S3 bucket. The interval at which it runs is specified in the module attributes.

---

## How It Works

1. **Interval**: The module’s scheduler runs on a configurable interval (in minutes).  
2. **Save Operation**: At each interval, it calls the `save` command on your configured video-store component to generate a video file.  
3. **Upload to S3**: The newly created `.mp4` file is then uploaded to the specified S3 bucket, after which the local file is removed to conserve disk space.

You can optionally include extra attributes (e.g. `customer`, `location`, `file_id`) to create a nested folder-like structure in your S3 bucket. (S3 does not have real folders; the slash `/` in object keys is interpreted as a directory separator in most S3 browsers.)

---

## Config Attributes

| **Name**         | **Type** | **Required?** | **Explanation**                                                                                             |
|------------------|----------|--------------|--------------------------------------------------------------------------------------------------------------|
| `video_store`    | string   | Yes          | The name of the video-store component                                                                       |
| `interval`       | int      | Yes          | The interval **in minutes** at which to call `save` and upload the file                                     |
| `local_path`     | string   | Yes          | Absolute path where the video-store component writes `.mp4` files                                           |
| `aws_region`     | string   | Yes          | The AWS region where your S3 bucket is located                                                              |
| `bucket_name`    | string   | Yes          | The **name** of your existing S3 bucket                                                                     |
| `aws_key_id`     | string   | Yes          | Your AWS Access Key ID                                                                                      |
| `aws_key_value`  | string   | Yes          | Your AWS Secret Access Key                                                                                  |
| `customer`       | string   | No           | (Optional) Used to form subfolders in your S3 bucket                                                        |
| `location`       | string   | No           | (Optional) Used to form subfolders in your S3 bucket                                                        |
| `file_id`         | string   | No           | (Optional) Used to form subfolders in your S3 bucket                                                        |

### Example Configuration

```jsonc
{
  "video_store": "my_video_store",
  "interval": 1,
  "local_path": "/var/data/videos",
  "aws_region": "us-east-2",
  "bucket_name": "example-bucket",
  "aws_key_id": "myAccessKeyID",
  "aws_key_value": "mySecretAccessKey",

  // Optional fields for custom subfolders:
  "customer": "some_customer",
  "location": "some_location",
  "file_id": "unique_file_id"
}
