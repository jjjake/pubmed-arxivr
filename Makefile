all:
	python pubmed/ingest.py pmc_results.txt

items:
	parallel 'pubmed/scripts/ingest-single-item.sh {}' < $(itemlist)

item:
	pubmed/scripts/ingest-single-item.sh $(id)

delete-from-dowehavit:
	pubmed/scripts/delete_items_from_dowehavit.sh $(items)
