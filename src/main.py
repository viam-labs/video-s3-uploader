import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import ClassVar, Final, Mapping, Sequence
import os
from datetime import datetime, timedelta
import time

from typing_extensions import Self
from viam.module.module import Module
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName
from viam.resource.base import ResourceBase
from viam.components.camera import Camera
from viam.resource.easy_resource import EasyResource
from viam.resource.types import Model, ModelFamily
from viam.services.generic import *
from viam import logging

import boto3

LOG = logging.getLogger(__name__)
MB = 1024 * 1024

class UploaderService(Generic, EasyResource):
    MODEL: ClassVar[Model] = Model(
        ModelFamily("ab2c1ad8-87cc-46c4-a981-a7dce5e07070", "video-s3-uploader"), "uploader-service"
    )
    
    aws_region: str = ""
    bucket_name: str = ""
    aws_secret_key_id: str = ""
    aws_secret_key_value: str = ""
    s3_client = None

    local_path: str = ""
    video_store: Camera = None

    # Example new fields for building a path
    customer: str = ""
    location: str = ""
    file_id: str = ""

    interval: int = 0
    scheduler: AsyncIOScheduler = None

    @classmethod
    def new(
        cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ) -> Self:
        """Create a new instance of this Generic service."""
        return super().new(config, dependencies)

    @classmethod
    def validate_config(cls, config: ComponentConfig) -> Sequence[str]:
        """Validate the configuration object and return any implicit dependencies."""
        validate_field_exists("aws_region", config)
        validate_field_exists("bucket_name", config)
        validate_field_exists("local_path", config)
        validate_field_exists("aws_key_id", config)
        validate_field_exists("aws_key_value", config)
        validate_field_exists("video_store", config)
        validate_field_exists("interval", config)

        # We introduce a few new optional fields for our subfolders
        # If these are truly optional, check before using them in reconfigure
        # or assign defaults if they don't exist:
        # validate_field_exists("customer", config)
        # validate_field_exists("location", config)
        # validate_field_exists("file_id", config)

        return [config.attributes.fields["video_store"].string_value]

    def reconfigure(
        self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ):
        """Update the service when it receives a new `config` object."""
        
        if self.scheduler is not None:
            self.scheduler.shutdown()
        else:
            self.scheduler = AsyncIOScheduler()

        self.local_path = config.attributes.fields["local_path"].string_value
        self.aws_region = config.attributes.fields["aws_region"].string_value
        self.bucket_name = config.attributes.fields["bucket_name"].string_value        
        self.aws_secret_key_id = config.attributes.fields["aws_key_id"].string_value
        self.aws_secret_key_value = config.attributes.fields["aws_key_value"].string_value

        # Get optional folder fields if they exist
        # For example, if you don't always have them in config:
        self.customer = config.attributes.fields.get("customer", None)
        if self.customer:
            self.customer = self.customer.string_value
        self.location = config.attributes.fields.get("location", None)
        if self.location:
            self.location = self.location.string_value
        self.file_id = config.attributes.fields.get("file_id", None)
        if self.file_id:
            self.file_id = self.file_id.string_value

        self.s3_client = boto3.resource(
            's3',
            aws_access_key_id=self.aws_secret_key_id,
            aws_secret_access_key=self.aws_secret_key_value,
            region_name=self.aws_region
        )
                
        video_store_name = config.attributes.fields["video_store"].string_value
        self.video_store = dependencies[Camera.get_resource_name(video_store_name)]
        
        self.interval = int(config.attributes.fields["interval"].number_value)
        
        self.start_upload_job()
        
    def start_upload_job(self):
        self.scheduler.add_job(self.upload, 'interval', minutes=self.interval)
        self.scheduler.start()
    
    async def save_video(self):
        to_time = datetime.now()
        to_string = to_time.strftime("%Y-%m-%d_%H-%M-%S")
        from_time = to_time - timedelta(minutes=self.interval)
        from_string = from_time.strftime("%Y-%m-%d_%H-%M-%S")
        LOG.info(f"calling save on video store module, from: {from_string} to: {to_string}")
        await self.video_store.do_command({
            "command": "save",
            "from": from_string,
            "to": to_string,
            "async": True
        })
    
    async def upload(self):
        await self.save_video()
        LOG.info("executing upload on folder")
        # Sleep briefly to allow file creation to finish
        time.sleep(15)
        files = []
        # Walk all dirs including nested ones and collect mp4 files
        for (root, dirs, file) in os.walk(self.local_path):
            for f in file:
                if f.endswith('.mp4'):
                    files.append((f, os.path.join(root, f)))

        for file, path in files:
            try:
                LOG.info(f"attempting s3 upload for file {path}")
                self.s3_upload(path, file)
                os.remove(path)
            except Exception as e:
                # If we specifically care about OSError:
                if isinstance(e, OSError):
                    LOG.warning(f"failed to get size of file {path}, skipping, error: {e}")
                    continue
                else:
                    LOG.warning(f"error uploading file to S3, error: {e}")
                    continue
    
    def s3_upload(self, file_path, file_name):
        """
        Upload a file from a local folder to an Amazon S3 bucket, placing it
        in a subfolder path that depends on 'customer', 'location', and 'file_id'.
        """
        # You can skip or conditionally build this path based on your config
        # Example:
        parts = []
        if self.customer:
            parts.append(self.customer)
        if self.location:
            parts.append(self.location)
        if self.file_id:
            parts.append(self.file_id)
        # Join them all (like "customer/location/file_id/filename.mp4"):
        # If you only want one prefix from config, just do: prefix = self.some_prefix
        s3_path = "/".join(parts)  # e.g. "customer/location/file_id"
        
        # Now, the final key is:  s3_path/filename.mp4
        if s3_path:
            object_key = f"{s3_path}/{file_name}"
        else:
            object_key = file_name  # no subfolders
        
        self.s3_client.Bucket(self.bucket_name).upload_file(file_path, object_key)
    
    async def close(self):
        if self.scheduler is not None:
            self.scheduler.shutdown()


def validate_field_exists(attribute_name: str, config: ComponentConfig):
    if attribute_name not in config.attributes.fields:
        raise Exception(f"{attribute_name} must be specified in config.")


if __name__ == "__main__":
    asyncio.run(Module.run_from_registry())
