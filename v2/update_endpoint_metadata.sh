#!/usr/bin/env bash
icgc-storage-client download --output-dir endpoint_metadata --object-id $1 --output-layout bundle --force > logs/force_update_metadata.txt