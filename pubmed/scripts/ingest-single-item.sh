#!/bin/bash

PMC="$(echo $1 | sed 's/^pubmed-//')"
medline_record="/tmp/${PMC}_medline.txt"
curl -L "http://www.ncbi.nlm.nih.gov/pmc/?term=${PMC}&report=MEDLINE&format=text" > /tmp/${PMC}_medline.txt
python pubmed/ingest.py /tmp/${PMC}_medline.txt
rm /tmp/${PMC}_medline.txt
