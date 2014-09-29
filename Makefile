item:
	curl -L 'http://www.ncbi.nlm.nih.gov/pmc/?term=$(PMC)&report=MEDLINE&format=text' > /tmp/$(PMC)_medline.txt
	python archive_pubmed.py /tmp/$(PMC)_medline.txt
	rm /tmp/$(PMC)_medline.txt

items:
	python pubmedrxivr/ingest.py pmc_results.txt

delete-from-dowehavit:
	echo pubmedrxivr/scripts/delete_items_from_dowehavit.sh $(items)
