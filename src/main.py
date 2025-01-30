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
    
    aws_region:str = ""
    bucket_name:str = ""
    aws_secret_key_id:str = ""
    aws_secret_key_value:str = ""
    s3_client = None

    local_path:str = ""
    video_store:Camera = None

    interval: int = 0
    scheduler: AsyncIOScheduler = None

    @classmethod
    def new(
        cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ) -> Self:
        """This method creates a new instance of this Generic service.
        The default implementation sets the name from the `config` parameter and then calls `reconfigure`.

        Args:
            config (ComponentConfig): The configuration for this resource
            dependencies (Mapping[ResourceName, ResourceBase]): The dependencies (both implicit and explicit)

        Returns:
            Self: The resource
        """
        return super().new(config, dependencies)

    @classmethod
    def validate_config(cls, config: ComponentConfig) -> Sequence[str]:
        """This method allows you to validate the configuration object received from the machine,
        as well as to return any implicit dependencies based on that `config`.

        Args:
            config (ComponentConfig): The configuration for this resource

        Returns:
            Sequence[str]: A list of implicit dependencies
        """
        validate_field_exists("aws_region", config)
        validate_field_exists("bucket_name", config)
        validate_field_exists("local_path", config)
        validate_field_exists("aws_key_id", config)
        validate_field_exists("aws_key_value", config)
        validate_field_exists("video_store", config)
        validate_field_exists("interval", config)
        # return the video store component name as an implicit dependency
        return [config.attributes.fields["video_store"].string_value]

    def reconfigure(
        self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ):
        """This method allows you to dynamically update your service when it receives a new `config` object.

        Args:
            config (ComponentConfig): The new configuration
            dependencies (Mapping[ResourceName, ResourceBase]): Any dependencies (both implicit and explicit)
        """
        
        if self.scheduler is not None:
            self.scheduler.shutdown()
        else:
            self.scheduler = AsyncIOScheduler()

        self.local_path = config.attributes.fields["local_path"].string_value

        self.aws_region = config.attributes.fields["aws_region"].string_value
        self.bucket_name = config.attributes.fields["bucket_name"].string_value        
        self.aws_secret_key_id = config.attributes.fields["aws_key_id"].string_value
        self.aws_secret_key_value = config.attributes.fields["aws_key_value"].string_value
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
        # sleep for 15 seconds to make sure file is present
        time.sleep(15)
        files = []
        # walk all dirs including nested ones and get a list of tuples containing (filename, filepath)
        for (root, dirs, file) in os.walk(self.local_path):
            for f in file:
                if '.mp4' in f:
                    files.append((f, os.path.join(self.local_path, f)))
        for file, path in files:
            try:
                LOG.info(f"attempting s3 upload for file {path}")
                self.s3_upload(path, file)
                os.remove(path)
            except Exception as e:
                if e == OSError:
                    LOG.warning(f"failed to get size of file {path}, skipping, error: {e}")
                    continue
                else:
                    LOG.warning(f"error uploading file to S3, error: {e}")
                    continue
    
    def s3_upload(self, file_path, object_key):
        """
        Upload a file from a local folder to an Amazon S3 bucket, using the default
        configuration.
        """
        self.s3_client.Bucket(self.bucket_name).upload_file(file_path, object_key)
    
    async def close(self):
        if self.scheduler is not None:
            self.scheduler.shutdown()

def validate_field_exists(attribute_name: str, config: ComponentConfig):
    if attribute_name not in config.attributes.fields:
        raise Exception(f"{attribute_name} must be specified in config.")

if __name__ == "__main__":
    asyncio.run(Module.run_from_registry())

