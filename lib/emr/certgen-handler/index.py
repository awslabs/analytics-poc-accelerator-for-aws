# Python Lambda handler that generates a self-signed certificate
# and uploads the resulting private key and certificate chain to
# a zip file in S3.

import datetime
import io
import os
import zipfile
from typing import TYPE_CHECKING

import boto3
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.types import (
    CertificateIssuerPrivateKeyTypes,
)
from cryptography.x509.oid import NameOID

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


class Key:
    """Private key"""

    def __init__(self) -> None:
        self._key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

    def key(self) -> CertificateIssuerPrivateKeyTypes:
        return self._key

    def bytes(self) -> bytes:
        return self._key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )


class SelfSignedCertificate:
    def __init__(self, key: CertificateIssuerPrivateKeyTypes, region: str) -> None:
        cn = (
            "*.ec2.internal"
            if region == "us-east-1"
            else "*.us-west-2.compute.internal"
        )
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Washington"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Seattle"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "EMR"),
                x509.NameAttribute(NameOID.COMMON_NAME, cn),
            ]
        )

        self._cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                # Our certificate will be valid for 90 days
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(days=90)
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                critical=False,
                # Sign our certificate with our private key
            )
            .sign(key, hashes.SHA256())
        )

    def bytes(self) -> bytes:
        return self._cert.public_bytes(serialization.Encoding.PEM)


def generate_physical_id(event) -> str:
    """Generates a physical resource ID from the inbound event"""
    return os.path.join(
        event["ResourceProperties"].get("bucket"),
        event["ResourceProperties"].get("prefix"),
    )


def create_zip(key: Key, cert: SelfSignedCertificate) -> io.BytesIO:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zipper:
        zipper.writestr("privateKey.pem", key.bytes())
        zipper.writestr("certificateChain.pem", cert.bytes())
    return zip_buffer


def upload_zip(client: S3Client, bucket: str, key: str, body: bytes):
    client.put_object(Bucket=bucket, Key=key, Body=body)


def on_success(id: str, bucket: str, key: str) -> dict:
    return {"PhysicalResourceId": id, "Data": {"s3Url": f"s3://{bucket}/{key}"}}


def handler(event, context):
    # Handle create and update statements similarly
    print(event)
    if event["RequestType"] in ["Create", "Update"]:
        print("Creating or updating certificates")
        bucket = event["ResourceProperties"].get("bucket")
        s3key = event["ResourceProperties"].get("prefix") + "certs.zip"
        physical_id = generate_physical_id(event)
        if (
            event["RequestType"] == "Update"
            and physical_id != event["PhysicalResourceId"]
        ):
            # delete the old resource
            print("Deleting old certificate from S3")

        key = Key()
        cert = SelfSignedCertificate(key.key(), "us-west-2")
        zipbytes = create_zip(key, cert)
        s3 = boto3.client("s3")
        upload_zip(s3, bucket, s3key, zipbytes.getvalue())
        return on_success(physical_id, bucket, s3key)
    elif event["RequestType"] == "Delete":
        print("Deleting certificate from S3")


if __name__ == "__main__":
    """Code for testing locally"""
    handler(
        {
            "RequestType": "Create",
            "ResourceProperties": {
                "bucket": "test-bucket",
                "key": "tmp/emrblob.zip",
            },
        },
        None,
    )
