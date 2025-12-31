#!/bin/bash

gcloud compute firewall-rules update prefect --source-ranges="$(curl -s ifconfig.me)/32"
