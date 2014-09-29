#!/bin/bash

itemlist="$1"

function delete_row() {
   pmc="$(echo $1 | sed 's/^pubmed-//')"
   sqlite3 dowehaveit.sqlite "DELETE FROM archived WHERE pmc='${pmc}';" 
}

if [ -f $itemlist ]; then
    for item in $(cat $itemlist); do
        delete_row $item
    done
else
    delete_row $itemlist
fi
