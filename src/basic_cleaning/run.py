#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import os
import tempfile
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def go(args):

    run = wandb.init(project='nyc_airbnb', job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("using artifcat: ", args.input_artifact)
    local_path = wandb.use_artifact(args.input_artifact).file()
    logger.info("cleaning data")
    df = pd.read_csv(local_path)
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    logger.info("df_shape: %s", str(df.shape))

    logger.info("fixing range for long, lat")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    with tempfile.TemporaryDirectory() as tmp_dir:
        logger.info("Creating tempdir to save csv")
        csv_path = os.path.join(tmp_dir, 'clean_sample.csv')
        df.to_csv(csv_path, index=False)
        logger.info("uploading artifact")
        artifact.add_file(csv_path)
        run.log_artifact(artifact)
        artifact.wait()
    run.finish()
    logger.info("done!")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Data to perform data cleaning",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Cleaned data artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='specify type for the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='short description for the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='minimum price of rent to filter',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='max price of house rent to filter',
        required=True
    )


    args = parser.parse_args()

    go(args)
