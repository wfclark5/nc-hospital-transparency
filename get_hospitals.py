#!/usr/bin/env python

# reading hospital names, urls, and identifiers.

import os
import pandas
import requests

path = os.getcwd()

df = pandas.read_csv(path + '/hospitals.csv')

# Create a data folder
if not os.path.exists('data'):
    os.mkdir('data')

# Create a subfolder for each
for hospital in df.hospital_id:
    if not os.path.exists('data/%s' % hospital):
        os.mkdir('data/%s' % hospital)

