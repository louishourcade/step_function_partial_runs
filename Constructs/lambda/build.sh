#!/bin/bash

rm -rf lambda_deploy
mkdir lambda_deploy
pip install -r requirements.txt -t lambda_deploy
cp velib_data_loader.py lambda_deploy
cd lambda_deploy
zip -r9 ../lambda_deploy.zip .
cd ..
rm -rf lambda_deploy