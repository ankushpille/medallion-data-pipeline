#!/usr/bin/env python3
"""
Deploy or update an AWS Lambda function from an ECR container image and create (or reuse)
a Function URL. Uses AWS credentials from environment variables or the usual
AWS config chain.

Usage:
    ./tools/deploy_lambda.py --image <ecr-image-uri> --name dea-lambda-fn --region us-east-1

If no --role-arn is provided the script will attempt to create an IAM role
named 'dea-lambda-role' with the AWSLambdaBasicExecutionRole managed policy.

Warning: Do NOT hardcode credentials in this script. Export them in your shell
before running (you already did):
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_SESSION_TOKEN=...

"""
import argparse
import boto3
import botocore.exceptions
import json
import os
import time
import sys
from pathlib import Path


def ensure_role(iam_client, role_name: str) -> str:
    try:
        resp = iam_client.get_role(RoleName=role_name)
        return resp["Role"]["Arn"]
    except iam_client.exceptions.NoSuchEntityException:
        pass

    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    print(f"Creating role {role_name}...")
    resp = iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role_policy))
    role_arn = resp["Role"]["Arn"]

    # Attach AWSLambdaBasicExecutionRole
    iam_client.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")

    # Wait for role propagation
    print("Waiting for role propagation (10s)...")
    time.sleep(10)
    return role_arn


def function_exists(lambda_client, name: str) -> bool:
    try:
        lambda_client.get_function(FunctionName=name)
        return True
    except lambda_client.exceptions.ResourceNotFoundException:
        return False


def deploy(args):
    # Support deploying either from a zip file (--zip) or an ECR image (--image).
    zip_path = None
    image_uri = args.image
    if not image_uri:
        zip_path = Path(args.zip)
        if not zip_path.exists():
            print(f"Zip file not found: {zip_path}")
            sys.exit(1)

    session = boto3.Session(region_name=args.region)
    lambda_client = session.client("lambda")
    iam_client = session.client("iam")

    role_arn = args.role_arn
    # If a default role ARN is provided via environment, prefer it.
    if not role_arn:
        role_arn = os.environ.get("DEFAULT_LAMBDA_ROLE_ARN")
    # If still not provided, attempt to create the role (may fail if caller lacks IAM perms)
    if not role_arn:
        try:
            role_arn = ensure_role(iam_client, args.role_name)
        except botocore.exceptions.ClientError as e:
            print("Failed to create role automatically. You can provide an existing role ARN with --role-arn or set DEFAULT_LAMBDA_ROLE_ARN in your environment.")
            raise

    fn_name = args.function_name

    code_bytes = None
    if zip_path:
        with open(zip_path, "rb") as f:
            code_bytes = f.read()

    if function_exists(lambda_client, fn_name):
        print(f"Function {fn_name} exists — updating code...")
        if image_uri:
            lambda_client.update_function_code(FunctionName=fn_name, ImageUri=image_uri, Publish=True)
        else:
            lambda_client.update_function_code(FunctionName=fn_name, ZipFile=code_bytes, Publish=True)

        # Build configuration kwargs and only include Layers if provided
        config_kwargs = dict(
            FunctionName=fn_name,
            Role=role_arn,
            Handler=args.handler,
            Runtime=args.runtime,
            Timeout=args.timeout,
            MemorySize=args.memory,
            Environment={"Variables": args.env_vars} if args.env_vars else {},
        )
        if args.layers:
            config_kwargs["Layers"] = args.layers
        lambda_client.update_function_configuration(**config_kwargs)
    else:
        print(f"Creating function {fn_name}...")
        if image_uri:
            create_kwargs = dict(
                FunctionName=fn_name,
                Role=role_arn,
                Code={"ImageUri": image_uri},
                PackageType='Image',
                Timeout=args.timeout,
                MemorySize=args.memory,
                Publish=True,
                Environment={"Variables": args.env_vars} if args.env_vars else {},
            )
            if args.layers:
                create_kwargs["Layers"] = args.layers
            lambda_client.create_function(**create_kwargs)
        else:
            create_kwargs = dict(
                FunctionName=fn_name,
                Runtime=args.runtime,
                Role=role_arn,
                Handler=args.handler,
                Code={"ZipFile": code_bytes},
                Timeout=args.timeout,
                MemorySize=args.memory,
                Publish=True,
                Environment={"Variables": args.env_vars} if args.env_vars else {},
            )
            if args.layers:
                create_kwargs["Layers"] = args.layers
            lambda_client.create_function(**create_kwargs)

    # If user asked to publish a local layer zip, do that and attach
    if args.publish_layer:
        layer_path = Path(args.publish_layer)
        if not layer_path.exists():
            print(f"Layer zip not found: {layer_path}")
            sys.exit(1)
        with open(layer_path, "rb") as lf:
            layer_bytes = lf.read()
        print(f"Publishing layer {layer_path.name}...")
        resp = lambda_client.publish_layer_version(
            LayerName=layer_path.stem,
            Content={"ZipFile": layer_bytes},
            CompatibleRuntimes=[args.runtime],
        )
        layer_arn = resp.get("LayerVersionArn")
        print(f"Published layer: {layer_arn}")
        # Attach this layer to the function (prepend to existing layers)
        current_layers = []
        try:
            cfg = lambda_client.get_function_configuration(FunctionName=fn_name)
            current_layers = cfg.get("Layers") or []
            current_layers = [l.get("Arn") for l in current_layers]
        except Exception:
            current_layers = []
        new_layers = [layer_arn] + current_layers
        lambda_client.update_function_configuration(FunctionName=fn_name, Layers=new_layers)

    # Create or update Function URL
    try:
        resp = lambda_client.get_function_url_config(FunctionName=fn_name)
        url = resp.get("FunctionUrl")
        print(f"Function URL exists: {url} — updating auth to NONE + CORS")
        lambda_client.update_function_url_config(
            FunctionName=fn_name,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["*"],
                "AllowMethods": ["GET", "POST", "OPTIONS"],
                "AllowHeaders": ["*"],
            },
        )
    except lambda_client.exceptions.ResourceNotFoundException:
        print("Creating Function URL with public access (AuthType=NONE)...")
        resp = lambda_client.create_function_url_config(
            FunctionName=fn_name,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["*"],
                "AllowMethods": ["GET", "POST", "OPTIONS"],
                "AllowHeaders": ["*"],
            },
        )
        url = resp.get("FunctionUrl")

    print("Deployment complete.")
    print(f"Function URL: {url}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--name", dest="function_name", required=True, help="Lambda function name")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--image", required=True, help="ECR image URI to deploy (required)")
    p.add_argument("--role-arn", dest="role_arn", default=None, help="Existing IAM role ARN to use")
    p.add_argument("--role-name", dest="role_name", default="dea-lambda-role", help="IAM role name to create if role-arn not provided")
    p.add_argument("--runtime", default="python3.10")
    p.add_argument("--handler", default="lambda_handler.handler")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--memory", type=int, default=512)
    p.add_argument("--env", action="append", default=[], help="Environment variables KEY=VALUE (can be repeated)")
    p.add_argument("--layers", default=None, help="Comma-separated existing LayerVersion ARNs to attach to the function")
    p.add_argument("--publish-layer", default=None, help="Path to a layer zip to publish and attach to the function")
    args = p.parse_args()
    # process env vars
    env_vars = {}
    for e in args.env:
        if "=" in e:
            k, v = e.split("=", 1)
            env_vars[k] = v
    args.env_vars = env_vars
    # process layers
    if args.layers:
        args.layers = [s.strip() for s in args.layers.split(",") if s.strip()]
    else:
        args.layers = []
    return args


if __name__ == "__main__":
    args = parse_args()
    try:
        deploy(args)
    except botocore.exceptions.ClientError as e:
        print("AWS ClientError:", e)
        sys.exit(1)
